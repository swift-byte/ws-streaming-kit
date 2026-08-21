import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Одно прочитанное имя из `Cookies`; `value` == nil — стораж ничего не отдал
private struct ReservedCookieRead: Sendable {
	let name: String
	let value: String?
}

actor WebSocketNetworkStreaming: NetworkStreaming {

	// MARK: - Private Types

	/// Вес очереди выходного стрима. Живёт отдельным типом, чтобы инвариант
	/// («в очереди лежат ровно последние `depth` записанных кадров») можно было
	/// проверить тестом, а не только глазами по циклу читателя.
	/// FIFO с головным индексом: removeFirst на каждый кадр давал бы
	/// O(n)-сдвиг ровно в том режиме, ради которого граница и заведена
	struct OutputQueueLedger {

		private var sizes = [Int]()
		private var head = 0
		private var bytes = 0

		/// - Parameter depth: сколько элементов лежит в очереди сейчас, включая
		///   не имеющий веса `.connected`; поэтому оценка веса завышена не
		///   более чем на один кадр, то есть срабатывает раньше, а не позже
		/// - Returns: вес очереди после учёта кадра
		mutating func record(size: Int, depth: Int) -> Int {
			// Инвариант держим сами, а не через политику буфера у вызывающего:
			// отрицательная глубина увела бы голову за конец массива
			let depth = max(0, depth)
			sizes.append(size)
			bytes += size
			while sizes.count - head > depth {
				bytes -= sizes[head]
				head += 1
			}
			// Сдвигаем, только когда мёртвая голова переросла саму очередь:
			// в здоровом режиме (потребитель успевает) очередь мелкая, голова
			// растёт на кадр за кадром, и сдвиг случается раз в depth кадров
			if head > depth {
				sizes.removeFirst(head)
				head = 0
			}
			return bytes
		}
	}

	/// Метка вытеснения. Ставит её тот, кто занимает место стрима; читают двое:
	/// connectTask вытесняемого (через `outcome(for:terminalError:)`) и сам
	/// establishStream, если вытеснение случилось в окне сбора кук и connectTask
	/// ещё не существует. Поле читается и пишется только в изоляции актора;
	/// @unchecked нужен, чтобы токен можно было захватить замыканием connectTask
	private final class LifecycleToken: @unchecked Sendable {
		var superseded = false
	}

	// MARK: - Private Properties

	/// Однобайтовый маркер конца ввода, который ждёт бэкенд: получив его,
	/// сервер перестаёт добирать аплинк и досылает остаток ответа. Значение —
	/// часть протокола, менять только вместе с сервером
	private static let inputTerminator = Data([0x31])
	private static let reservedCookieNames = Set(Cookies.allCases.map { $0.rawValue.lowercased() })

	/// Граница ожидания дренажа: по истечении стрим финиширует, не дожидаясь
	/// читателя. В happy path снимается сразу, как только читатель завершился
	private static let drainGraceNanoseconds: UInt64 = 3_000_000_000

	/// Потолок кадра. Совпадает с платформенным дефолтом и задаётся явно, чтобы
	/// верхняя граница памяти была видна в коде: кадр больше потолка закрывает
	/// соединение кодом 1009
	private static let maximumMessageSize = 1024 * 1024

	/// Границы очереди выходного стрима. Контракт прежний — потребитель обязан
	/// вычитывать стрим, backpressure нет; границы нужны, чтобы залипший
	/// потребитель или заливающий сервер давали наблюдаемый отказ, а не рост
	/// до jetsam. Считаются обе: по кадрам и по байтам. Одной глубины мало —
	/// 1024 кадра по maximumMessageSize это уже гигабайт, который iOS не
	/// переживёт; одних байтов мало — AsyncStream умеет ограничивать только
	/// число элементов, и байтовый счёт снимается с его же remaining
	private static let outputBufferDepth = 1024
	private static let outputBufferBytes = 8 * 1024 * 1024

	/// Чтение кук должно быть мгновенным; столько ждать его уже патология
	private static let cookieCollectionNanoseconds: UInt64 = 5_000_000_000

	/// Платформенный дедлайн простоя — последняя страховка от соединения,
	/// умершего без FIN: сокет, молчащий дольше, CFNetwork закроет сам, и
	/// потребитель увидит `.nsError`.
	/// Задаётся явно ради одного инварианта: платформа не должна опережать наш
	/// собственный дедлайн, иначе `timeout` больше `timeoutIntervalForRequest`
	/// не значил бы ничего. Цена выбора названа прямо: пауза в диалоге,
	/// пережившая `invalidate()`, ограничена этим же числом, а мёртвое
	/// соединение замечается за то же время. Ping/pong снял бы размен, но
	/// проверить его на целевой платформе из этого окружения нечем
	private static func platformRequestTimeout(for timeout: Int) -> TimeInterval {
		max(TimeInterval(timeout) * 2, 300)
	}

	private let cookieStorage: CookieStorage
	private let timeout: Int
	private let allowsInsecureEndpoint: Bool
	private let session: URLSession
	/// Снимается при инициализации с того же объекта конфигурации, что получила
	/// сессия: разойтись они не могут (сессия делает свой снимок там же), а
	/// session.configuration отдаёт КОПИЮ конфигурации на каждое обращение
	private let handshakeCookieStorage: HTTPCookieStorage?

	private var generation = 0
	private var currentToken: LifecycleToken?
	private var isTimeoutArmed = true
	private var webSocketTask: URLSessionWebSocketTask?
	private var outputTask: Task<Void, Never>?
	private var inputTask: Task<Void, Never>?
	private var connectTask: Task<Void, Never>?
	private var timer: Task<Void, Never>?

	// MARK: - Init

	/// - Parameter timeout: секунды ожидания ПЕРВОГО входящего сообщения
	///   (не только рукопожатия); значения вне 1...3600 клампятся к границам.
	///   Отсчёт начинается ПОСЛЕ сбора кук, поэтому свой сторожевой таймер
	///   вызывающему стоит ставить с запасом на `cookieCollectionNanoseconds`.
	/// - Parameter allowsInsecureEndpoint: разрешает ws:// вне loopback.
	///   Включать только там, где TLS терминируется вне приложения (on-prem,
	///   стенд в приватной сети): SDK-куки пойдут открытым текстом. Решение
	///   принадлежит конфигурации — рантайм лишь исполняет его.
	init(
		kidsURLSession: KidsURLSession,
		cookieStorage: CookieStorage,
		timeout: Int? = nil,
		allowsInsecureEndpoint: Bool = false
	) {
		self.cookieStorage = cookieStorage
		self.allowsInsecureEndpoint = allowsInsecureEndpoint
		// Нижняя граница спасает UInt64-конверсию от отрицательного значения
		// (иначе трап); верхняя — санитарный предел
		self.timeout = min(max(1, timeout ?? 15), 3_600)

		let configuration = URLSessionConfiguration.default
		configuration.httpCookieStorage = .shared
		configuration.timeoutIntervalForRequest = Self.platformRequestTimeout(for: self.timeout)
		handshakeCookieStorage = configuration.httpCookieStorage
		session = URLSession(
			configuration: configuration,
			delegate: kidsURLSession,
			delegateQueue: nil
		)
	}

	// MARK: - Deinit

	// Continuations финиширует connectTask — на всех своих путях выхода, и
	// ранних return внутри цикла у него нет; deinit ссылок на них не имеет,
	// поэтому отмена задач здесь и приводит стрим к тихому финишу
	deinit {
		// Порядок как в cancel(generation:): ввод — до разрыва сокета, иначе
		// inflight-send эскалирует штатное разрушение как сбой аплинка
		inputTask?.cancel()
		webSocketTask?.cancel(with: .normalClosure, reason: nil)
		outputTask?.cancel()
		connectTask?.cancel()
		timer?.cancel()
		// finishTasksAndInvalidate, а не invalidateAndCancel: последний рвал
		// соединение раньше, чем улетал close-фрейм строкой выше
		session.finishTasksAndInvalidate()
	}

	// MARK: - NetworkStreaming

	/// Устанавливает соединение и возвращает стрим событий.
	///
	/// Порядок событий: `.connected`, затем `.received` в порядке прихода.
	/// Исходы стрима:
	/// - тихий `finish()` — штатное закрытие сервером (в том числе close-фрейм
	///   без кода, 1005: по RFC 6455 7.1.5 это завершённое закрытие, а не сбой),
	///   явный `cancel()`, если причина к тому моменту ещё не известна, или
	///   деинициализация актора;
	/// - `CancellationError` — стрим вытеснен следующим `establishStream`;
	/// - `NetworkStreamingError.timeout` — ЛИБО первое сообщение не пришло за
	///   `timeout` секунд (этот дедлайн тикает и после `.connected`, снимается
	///   через `invalidate()`), ЛИБО соединение потеряно: унаследованный
	///   маппинг NSURLErrorNetworkConnectionLost, см. `transportFailure`.
	///   Второй вариант приходит когда угодно и `invalidate()` его не снимает;
	/// - `NetworkStreamingError.closeCode` — сервер закрыл соединение с кодом;
	/// - `NetworkStreamingError.nsError` — прочая транспортная ошибка, в том
	///   числе переполнение очереди выходного стрима, отказанный редирект и
	///   платформенный дедлайн простоя (см. `invalidate()`).
	///
	/// - Каждому вызову нужен свежий inputStream: AsyncStream одноразов и
	///   single-consumer, повторная передача делит данные недетерминированно.
	/// - До `.connected` inputStream не потребляется; при отказе на этой фазе он
	///   освобождается, продюсеру приходит onTermination.
	/// - Отказы валидации endpoint — единственные без teardown: действующий
	///   стрим сохраняется.
	/// - Прекращение чтения выходного стрима потребителем закрывает соединение:
	///   стрим и есть владение им.
	/// - Пара из `headers` не уходит на провод, если имя не RFC 7230 token,
	///   значение содержит управляющие символы, имя принадлежит транспорту
	///   (Connection, Upgrade, Host, Content-Length, Transfer-Encoding,
	///   Sec-WebSocket-*, Cookie) либо это регистронезависимый дубль. Каждый
	///   случай логируется — молча не теряется ничего.
	/// - Сбой отправки во входном стриме закрывает соединение и пишется в лог
	///   (домен и код), но СВОИМ исходом не становится: `send` падает лишь
	///   когда транспорт уже кончился, а назвать причину может только делегат —
	///   он один видит close-код. Цена размена: если транспорт завершился
	///   чисто, обрыв аплинка виден только в логе. Собственный исход здесь
	///   гонялся бы с close-фреймом и отбирал у потребителя причину сервера.
	/// - Сбор кук ограничен своим дедлайном: по его истечении рукопожатие
	///   уходит без авторизации, но `establishStream` не зависает.
	/// - Если `cookieStorage` не отдал зарезервированную куку, рукопожатие уйдёт
	///   без неё, и отказ придёт от сервера: `.closeCode`, если тот принял
	///   апгрейд и закрыл сокет, либо `.nsError`, если отказал ещё на HTTP.
	func establishStream(
		endpoint: String,
		headers: [String: String],
		inputStream: AsyncStream<Data>
	) async throws -> AsyncThrowingStream<AssistantSDK.NetworkStreamingOutputEvent, any Error> {
		let url = try Self.validate(endpoint: endpoint, allowsInsecure: allowsInsecureEndpoint)

		// Вытеснение: помечается токен текущего владельца — живого стрима или
		// вызова в окне кук. Глобальной бухгалтерии нет: токен умирает вместе
		// со своим стримом, протухать нечему, затирать нечего
		currentToken?.superseded = true
		// Приватный teardown напрямую: неразрывность «пометка → teardown →
		// чтение поколения» структурная, а не комментарийная
		cancel(generation: generation)
		let currentGeneration = generation
		let lifecycleToken = LifecycleToken()
		currentToken = lifecycleToken

		let (outputStream, outputContinuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>
			.makeStream(bufferingPolicy: .bufferingOldest(Self.outputBufferDepth))

		// Потребитель бросил стрим — значит, соединение больше некому читать:
		// закрываем его. Поколение в захвате делает хук безвредным для всех
		// последующих стримов
		outputContinuation.onTermination = { [weak self] _ in
			Task { await self?.cancel(generation: currentGeneration) }
		}

		var request = URLRequest(url: url)
		// Порядок обхода словаря не определён, а имена заголовков
		// регистронезависимы: без сортировки и учёта уже занятых имён пара
		// ["X-Trace": a, "x-trace": b] уходила бы на провод по-разному от
		// запуска к запуску, и снова молча
		var claimedHeaderNames = Set<String>()
		for (key, value) in headers.sorted(by: { $0.key < $1.key }) {
			guard Self.isToken(key), Self.isValidHeaderValue(value) else {
				// Значение с CR/LF Foundation отбрасывает вместе со всем
				// заголовком; имя с управляющими символами она, наоборот,
				// ставит дословно и пустила бы на провод. Оба исхода одинаково
				// невидимы без лога. Имя логируется очищенным: CR/LF в нём
				// подделали бы запись лога
				Logger.assistant.error(S("Header rejected: \(Self.logSafe(key))"))
				continue
			}
			guard Self.isReservedHeaderName(key) == false else {
				Logger.assistant.error(S("Handshake header owned by URLSession, ignored: \(Self.logSafe(key))"))
				continue
			}
			guard claimedHeaderNames.insert(key.lowercased()).inserted else {
				Logger.assistant.error(S("Duplicate header name, ignored: \(Self.logSafe(key))"))
				continue
			}
			request.setValue(value, forHTTPHeaderField: key)
		}

		await addCookies(to: &request, for: url)

		guard generation == currentGeneration else {
			// Пока собирали куки, поколение сдвинулось. Причину различаем по
			// токену: вытеснение — CancellationError (ретрай-логика не должна
			// путать замену со штатным закрытием), явная отмена — тихий finish,
			// единый с контрактом поднятого стрима
			outputContinuation.onTermination = nil
			if lifecycleToken.superseded {
				outputContinuation.finish(throwing: CancellationError())
			} else {
				outputContinuation.finish()
			}
			return outputStream
		}

		let (delegateStream, delegateContinuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.makeStream()

		let delegate = WebSocketNetworkStreamingDelegate(origin: url, continuation: delegateContinuation)
		let task = session.webSocketTask(with: request)
		task.maximumMessageSize = Self.maximumMessageSize
		webSocketTask = task
		task.delegate = delegate
		task.resume()

		// Страховка могла быть снята вызовом invalidate() в окне сбора кук —
		// тогда таймер не заводится вовсе
		if isTimeoutArmed {
			let timeoutSeconds = UInt64(timeout)
			timer = Task { [weak self] in
				try? await Task.sleep(nanoseconds: timeoutSeconds * 1_000_000_000)
				if Task.isCancelled { return }
				// Проверка «был ли первый байт» и финиш — одним изолированным
				// шагом: разнесённые check и act оставляли окно, где .timeout
				// перебивал уже принятое сообщение
				await self?.fireTimeout(
					generation: currentGeneration,
					outputContinuation: outputContinuation,
					delegateContinuation: delegateContinuation
				)
			}
		}

		createConnectTask(
			generation: currentGeneration,
			lifecycleToken: lifecycleToken,
			task: task,
			inputStream: inputStream,
			delegateStream: delegateStream,
			delegateContinuation: delegateContinuation,
			outputContinuation: outputContinuation
		)

		return outputStream
	}

	/// Закрывает ТЕКУЩЕЕ соединение актора и отменяет его задачи. Стрим
	/// завершается тихо, если причина к этому моменту ещё не известна; уже
	/// наблюдённая (close-код сервера, транспортная ошибка) сохраняется —
	/// вызвавший отмену вправе её проигнорировать. Идентичности у стримов нет — сигнатура
	/// унаследована от протокола, — поэтому вызов, разошедшийся с новым
	/// `establishStream`, закроет уже новый стрим, и тот финиширует, не отдав
	/// ни одного события; вызывающему стоит упорядочивать отмену и переподъём.
	/// Хвост входящих при отмене НЕ гарантирован: тот же teardown отменяет
	/// читателя, и уже принятый, но ещё не отданный кадр теряется. При закрытии
	/// по инициативе сервера хвост дожидается читателя, но не дольше
	/// `drainGraceNanoseconds` — после чего стрим финиширует без него.
	func cancel() async {
		cancel(generation: generation)
	}

	// MARK: - Deadline Control

	/// Снимает дедлайн ожидания первого входящего сообщения. Дедлайн тикает и
	/// после `.connected`, так что снять его — единственный способ пережить
	/// долгую паузу в диалоге.
	///
	/// Платформенный дедлайн простоя при этом ОСТАЁТСЯ и снять его нечем:
	/// соединение, молчащее дольше `platformRequestTimeout(for:)`, CFNetwork
	/// закроет сам, и потребитель увидит `.nsError` (NSURLErrorTimedOut).
	/// Это осознанный размен: без него соединение, умершее без FIN, висело бы
	/// вечно. Приложению, которому нужны более долгие паузы, придётся греть
	/// соединение своим трафиком либо поднять `timeout` при инициализации.
	///
	/// В протокол `NetworkStreaming` не входит — доступен по конкретному типу.
	func invalidate() {
		disarmTimeout()
	}

	/// Разоружение коммитится в том же изолированном шаге, что и отмена
	/// хэндла: иначе оставалось окно в один хоп, где уже проснувшийся таймер
	/// добегал до fireTimeout и рвал стрим после снятия страховки.
	/// Проверки поколения здесь нет намеренно: все вызывающие либо сами уже
	/// его сверили, либо разоружают текущее
	private func disarmTimeout() {
		isTimeoutArmed = false
		timer?.cancel()
		timer = nil
	}

	// MARK: - Endpoint Validation

	/// Отбраковывает endpoint до любого teardown'а. Internal, а не private,
	/// ради прямого теста без поднятия соединения.
	/// - `.badURL` — строка не разобралась либо в ней нет схемы или хоста.
	/// - `.unsupportedURL` — схема не ws/wss. Вебсокет-задача определена только
	///   на них (замерено: с http:// рукопожатие просто не отвечает), а поздний
	///   отказ пришёл бы уже после teardown действующего стрима.
	/// - `.appTransportSecurityRequiresSecureConnection` — ws вне loopback без
	///   явного разрешения: SDK-куки ушли бы открытым текстом и достались бы
	///   любому на пути.
	static func validate(endpoint: String, allowsInsecure: Bool = false) throws -> URL {
		guard let url = URL(string: endpoint),
			  let scheme = url.scheme?.lowercased(),
			  let host = url.host,
			  host.isEmpty == false else {
			throw URLError(.badURL)
		}
		switch scheme {
		case "wss":
			return url
		case "ws":
			guard allowsInsecure || isLoopback(host: host) else {
				Logger.assistant.error(S("Insecure endpoint rejected: \(logSafe(host))"))
				throw URLError(.appTransportSecurityRequiresSecureConnection)
			}
			return url
		default:
			throw URLError(.unsupportedURL)
		}
	}

	/// Один вид хоста на все сравнения: регистр, скобки IPv6-литерала и
	/// корневая точка не должны делать один и тот же хост двумя разными
	static func canonicalHost(_ host: String) -> String {
		var canonical = host.lowercased().trimmingCharacters(in: CharacterSet(charactersIn: "[]"))
		if canonical.hasSuffix(".") { canonical.removeLast() }
		return canonical
	}

	/// Покрывает формы, в которых локальный бэкенд реально встречается: весь
	/// 127.0.0.0/8 (алиасы вида 127.0.0.2 — штатный способ развести несколько
	/// сервисов), ::1 и localhost. Экзотику вроде ::ffff:127.0.0.1 сюда не
	/// тащим: отказ виден сразу и лечится переходом на wss
	private static func isLoopback(host: String) -> Bool {
		let canonical = canonicalHost(host)
		if canonical == "localhost" || canonical == "::1" || canonical == "0:0:0:0:0:0:0:1" {
			return true
		}
		let octets = canonical.split(separator: ".", omittingEmptySubsequences: false)
		guard octets.count == 4, octets.allSatisfy({ UInt8($0) != nil }) else { return false }
		return octets[0] == "127"
	}

	// MARK: - Private Methods

	// Тело обязано оставаться синхронным (без await): establishStream
	// полагается на неразрывность «пометка токена → teardown → чтение
	// поколения» внутри своей изоляции.
	// closeCode — доставка замерена: iOS 1001, macOS всегда 1006 (дефект
	// платформы), Linux недетерминирован (0/1001) — на целевой iOS работает
	private func cancel(
		generation requested: Int,
		closeCode: URLSessionWebSocketTask.CloseCode = .normalClosure
	) {
		guard requested == generation else { return }
		// Отмена ввода — до разрыва сокета: inflight-send иначе падал от
		// разрыва раньше, чем задача видела отмену, и штатный teardown
		// эскалировался как сбой аплинка
		inputTask?.cancel()
		inputTask = nil
		webSocketTask?.cancel(with: closeCode, reason: nil)
		webSocketTask = nil
		// Взводим обратно, потому что слот освобождается вместе с остальными:
		// следующий стрим начинается со страховкой, а снятие, сделанное для
		// прошлого, на него не переносится
		isTimeoutArmed = true
		outputTask?.cancel()
		outputTask = nil
		connectTask?.cancel()
		connectTask = nil
		timer?.cancel()
		timer = nil
		// Токен мёртвого стрима не доживает до следующего establish: иначе
		// ретрай «cancel → establish» пометил бы его вытесненным, и потребитель,
		// сам вызвавший отмену, получил бы CancellationError
		currentToken = nil
		// Любой teardown двигает поколение: отложенные хопы, токены и таймеры
		// прошлых состояний инвалидируются разом, а отмена в окне addCookies
		// ловится supersede-guard'ом так же, как вытеснение
		generation &+= 1
	}

	private func yieldConnectedIfCurrent(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> Bool {
		guard requested == generation else { return false }
		outputContinuation.yield(.connected)
		return true
	}

	/// Исход стрима — одним изолированным шагом: флаг токена живёт в изоляции
	/// актора (дисциплина @unchecked требует того же). Вытеснение перебивает
	/// причину от делегата, потому что подменившему стриму нужен однозначный
	/// CancellationError — иначе ретрай-логика примет замену за закрытие.
	/// Флаг читается безусловно: пометка (establishStream) и отмена connectTask
	/// разнесены внутри cancel(generation:), так что проверка Task.isCancelled
	/// здесь проиграла бы гонку и вытеснение выглядело бы тихим финишем.
	/// nil — тихий финиш
	private func outcome(for token: LifecycleToken, terminalError: Error?) -> Error? {
		if token.superseded { return CancellationError() }
		return terminalError
	}

	private func acceptFirstMessage(generation requested: Int) -> Bool {
		// Опоздавший байт отсекает поколение: fireTimeout двигает его в том же
		// изолированном шаге, что и решение о таймауте
		guard requested == generation else { return false }
		disarmTimeout()
		return true
	}

	private func fireTimeout(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		guard requested == generation, isTimeoutArmed else { return }
		failStream(
			generation: requested,
			outputContinuation: outputContinuation,
			delegateContinuation: delegateContinuation,
			error: .timeout
		)
	}

	/// Единственная точка, где отказ порождает САМ актор (таймаут, переполнение
	/// очереди) — причины, наблюдённые снаружи, приезжают через delegateStream.
	/// Причина фиксируется на обеих continuation до разрыва сокета: иначе гонка
	/// didClose/didComplete успела бы подменить её на «сервер договорил».
	/// Закрываемся .goingAway, а не .normalClosure — иначе сервер записал бы
	/// упавший стрим как штатно завершённый клиентом.
	/// Снятый onTermination убирает Task с cancel(.normalClosure), который
	/// гонялся бы с нашим teardown и мог перебить close-код
	private func failStream(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		error: NetworkStreamingError
	) {
		guard requested == generation else { return }
		outputContinuation.onTermination = nil
		outputContinuation.finish(throwing: error)
		delegateContinuation.finish(throwing: error)
		cancel(generation: requested, closeCode: .goingAway)
	}

	private func createInputTask(
		generation requested: Int,
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>
	) {
		guard requested == generation else { return }
		// detached с [weak self]: тело почти не трогает актора, а Task {} с
		// наследованием изоляции на части компиляторов неявно захватывает self
		// сильно — незавершающийся inputStream удерживал бы актора от deinit
		inputTask = Task.detached { [weak self] in
			var sendFailed = false
			for await data in inputStream {
				if sendFailed { continue } // дожигаем ввод: буфер продюсера не растёт
				do {
					try await task.send(.data(data))
				} catch {
					// Штатный teardown: сокет рвётся раньше, чем send видит
					// отмену — это не сбой аплинка
					guard Task.isCancelled == false else { return }
					sendFailed = true
					await self?.noteUplinkFailure(generation: requested, error: error as NSError)
					// Ещё одна проверка после хопа: пока мы ходили на актора,
					// teardown мог закрыть сокет сам, и второе закрытие только
					// перебило бы его код своим
					guard Task.isCancelled == false else { return }
					// .normalClosure, а не .goingAway: закрытие по своей
					// инициативе выставляет closeCode задачи (замерено: 1001),
					// а делегат считает код закрытия словом сервера. Чистый код
					// не искажает картину; на проводе разницы нет — close-фрейм
					// при cancel(with:) всё равно не доставляется
					task.cancel(with: .normalClosure, reason: nil)
				}
			}
			guard Task.isCancelled == false, sendFailed == false else { return }
			Logger.assistant.info(S("Input stream finished"))
			do {
				try await task.send(.data(Self.inputTerminator))
			} catch {
				// Тот же путь, что и у обычной отправки: без терминатора сервер
				// продолжает добирать аплинк и может не ответить вовсе, так что
				// оставлять соединение открытым здесь нельзя
				guard Task.isCancelled == false else { return }
				await self?.noteUplinkFailure(generation: requested, error: error as NSError)
				guard Task.isCancelled == false else { return }
				task.cancel(with: .normalClosure, reason: nil)
			}
		}
	}

	/// Сбой отправки — симптом, а не причина: send падает ровно тогда, когда
	/// транспорт уже кончился, а исход транспорта называет делегат, и только он
	/// видит close-код сервера. Поэтому здесь остаётся лог, а не собственный
	/// finish: тот выигрывал гонку у didCloseWith и подменял исход (замерено:
	/// с активным вводом /close1011 отдавал .timeout вместо closeCode(1011),
	/// а штатный /close1000 — .timeout вместо тихого финиша; 6/6 и 8/8).
	/// Страховку снимаем: соединение уже рвётся, и таймаут, выстрелив в окне
	/// дренажа, подменил бы причину точно так же
	private func noteUplinkFailure(generation requested: Int, error: NSError) {
		guard requested == generation else { return }
		Logger.assistant.error(S("WebSocket input send failed: \(error.domain) \(error.code)"))
		disarmTimeout()
	}

	/// Заводит читателя и отдаёт сигнал его завершения; nil — поколение сдвинулось.
	private func createOutputTask(
		generation requested: Int,
		task: URLSessionWebSocketTask,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> AsyncStream<Void>? {
		// Хоп отменённой connectTask всё равно исполняется: без guard'а поздний
		// вызов затёр бы outputTask нового поколения — тот остался бы живым,
		// но без ссылки, и teardown его уже не отменил бы
		guard requested == generation else { return nil }
		let (done, doneContinuation) = AsyncStream<Void>.makeStream()
		let reader = Task<Void, Never> { [weak self] in
			// Сигнал завершения — от самого читателя: ожидание в drainOutput
			// становится отменяемым, промежуточный waiter не нужен
			defer { doneContinuation.finish() }
			var isFirstMessage = true
			var warnedTextFrame = false
			var warnedUnknownFrame = false
			var ledger = OutputQueueLedger()
			do {
				while Task.isCancelled == false {
					let message = try await task.receive()
					let payload: Data?
					switch message {
					case .string(let text):
						if warnedTextFrame == false {
							warnedTextFrame = true
							Logger.assistant.error(S("We expect binary data here"))
						}
						payload = Data(text.utf8)
					case .data(let data):
						payload = data
					@unknown default:
						// Единственный источник nil ниже. Кадр платформа
						// доставила, а отдать его нечем — по правилу «молча
						// терять кадры нельзя» хотя бы логируем, один раз
						if warnedUnknownFrame == false {
							warnedUnknownFrame = true
							Logger.assistant.error(S("Unknown WebSocket frame kind, dropped"))
						}
						payload = nil
					}
					// Таймер гасит первый кадр, который ЕСТЬ ЧЕМ отдать:
					// текстовый конвертируется (с error-логом о нарушении
					// binary-контракта) — значит, тоже гасит; @unknown-кадр
					// отдать нечем — не гасит. Разоружение идёт до yield:
					// иначе переполнение очереди на первом же кадре оставило бы
					// страховку взведённой поверх уже объявленного отказа
					guard let payload else { continue }
					if isFirstMessage {
						isFirstMessage = false
						// Арбитраж «первый байт против таймаута» — одна точка
						// решения на акторе, победитель ровно один: либо таймаут
						// (сообщение считается опоздавшим и не доставляется),
						// либо первый байт (таймаут уже не выстрелит и все
						// последующие yield безопасны). Смешанного исхода
						// «данные, затем .timeout» не существует
						guard await self?.acceptFirstMessage(generation: requested) == true else {
							return
						}
					}
					// Очередь ограничена и по кадрам, и по байтам: `remaining`
					// даёт точную глубину, а значит в очереди лежат ровно
					// последние `depth` наших yield'ов — по ним и считается вес.
					// Оценка консервативна (`.connected` занимает слот, но веса
					// не имеет), то есть срабатывает чуть раньше, а не позже
					switch outputContinuation.yield(.received(payload)) {
					case .enqueued(let remaining):
						let queuedBytes = ledger.record(
							size: payload.count,
							depth: Self.outputBufferDepth - remaining
						)
						guard queuedBytes > Self.outputBufferBytes else { continue }
					case .dropped:
						break
					case .terminated:
						// Стрим уже завершён кем-то ещё — читать больше некому
						return
					@unknown default:
						continue
					}
					// Молча терять кадры нельзя: стрим падает наблюдаемо
					Logger.assistant.error(S("Output buffer overflow: consumer is not draining"))
					await self?.failStream(
						generation: requested,
						outputContinuation: outputContinuation,
						delegateContinuation: delegateContinuation,
						error: .nsError(
							NSError(domain: NSURLErrorDomain, code: NSURLErrorDataLengthExceedsMaximum)
						)
					)
					return
				}
			} catch {
				// Терминальная ошибка чтения: завершение стрима — за connectTask,
				// который сперва дожидается доставки хвоста сообщений здесь
			}
		}
		outputTask = reader
		return done
	}

	private func createConnectTask(
		generation requested: Int,
		lifecycleToken: LifecycleToken,
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>,
		delegateStream: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		connectTask = Task { [weak self] in
			var readerDone: AsyncStream<Void>?
			var terminalError: Error?

			// Финиш стрима — только после доставки хвоста читателем. Ожидание
			// ограничено гонкой «сигнал завершения читателя против дедлайна»:
			// обе ветви группы отменяемы, группа не зависает. Патология
			// (receive() не проснулся на закрытом сокете) оставляет висеть
			// только сам читатель — минимально возможная цена.
			// На отменённых путях дренаж вырождается в no-op сам: дочерние задачи
			// группы наследуют отмену и завершаются мгновенно (замерено). Это и
			// правильно — тот же teardown отменил читателя строкой раньше
			func drainOutput() async {
				guard let readerDone else { return }
				await withTaskGroup(of: Void.self) { group in
					group.addTask { for await _ in readerDone {} }
					group.addTask {
						try? await Task.sleep(nanoseconds: Self.drainGraceNanoseconds)
					}
					await group.next()
					group.cancelAll()
				}
			}

			do {
				for try await event in delegateStream {
					if case .connected = event, readerDone == nil {
						// .connected выдаётся одним изолированным шагом с проверкой
						// поколения; reader создаётся после yield, иначе .received
						// мог бы обогнать .connected
						guard await self?.yieldConnectedIfCurrent(
							generation: requested,
							outputContinuation: outputContinuation
						) == true else {
							continue
						}
						// self связывается на время настройки — окно короткое и
						// заканчивается вместе с этим блоком, актор оно не удержит
						guard let self else { continue }
						guard let created = await self.createOutputTask(
							generation: requested,
							task: task,
							delegateContinuation: delegateContinuation,
							outputContinuation: outputContinuation
						) else {
							// Отказ означает teardown в окне хопа: ранний return
							// подвесил бы continuation, которую может дочитывать
							// потребитель, — ждём штатного финиша через delegateStream
							continue
						}
						readerDone = created
						await self.createInputTask(
							generation: requested,
							task: task,
							inputStream: inputStream
						)
					}
				}
			} catch {
				terminalError = error
			}
			await drainOutput()
			// Вытеснение перебивает всё: подменившему стриму нужен однозначный
			// CancellationError, иначе ретрай-логика примет замену за закрытие.
			// Дальше — уже наблюдённая причина: явная отмена сама по себе
			// терминальной ошибки не порождает (наше закрытие делегат мапит в
			// чистый финиш), так что дошедший сюда terminalError возник ДО
			// отмены, и гасить его нечем — вызвавший отмену вправе его
			// проигнорировать.
			// Исход читается ДО снятия onTermination, чтобы между снятием и
			// финишем не осталось точки приостановки. Снимаем же потому, что
			// иначе финиш планирует Task с cancel() того же поколения, который
			// следующей строкой отрабатывает и сам — хоп холостой
			// ?? nil схлопывает двойную опциональность (актор мог уйти) в одну:
			// без него `if let` снял бы лишь внешний уровень и «тихий финиш»
			// стало бы невозможно отличить от «актора нет»
			let outcome = await self?.outcome(for: lifecycleToken, terminalError: terminalError) ?? nil
			outputContinuation.onTermination = nil
			outputContinuation.finish(throwing: outcome)
			if Task.isCancelled == false {
				await self?.cancel(generation: requested)
			}
		}
	}

	// MARK: - Cookies

	// Internal, а не private, ради адресных тестов на сборку куки и заголовка
	func addCookies(to request: inout URLRequest, for url: URL) async {
		// Cookie-заголовок вызывающего снимается безусловно — заголовком владеет
		// только этот пайплайн. Авто-обработку отключаем: иначе URLSession может
		// перетереть собранный вручную Cookie значениями из shared-стоража.
		// ВАЖНО: это отключает и сохранение Set-Cookie из ответа на handshake —
		// если бэк ротирует куки на рукопожатии, их нужно подхватывать отдельно
		request.httpShouldHandleCookies = false
		request.setValue(nil, forHTTPHeaderField: "Cookie")

		// Слияние вместо замещения: свои куки перекрывают одноимённые из
		// shared-стоража, остальные сохраняются. Ключ — имя, намеренно без
		// path: path-scoped дубликат положил бы протухшее значение рядом со
		// свежим. Схема нормализуется (ws→http, wss→https), чтобы матчинг
		// стоража и secure-куки работали
		var merged = [String: HTTPCookie]()

		// Зарезервированные имена не берутся из shared вовсе: чужая кука
		// хост-приложения не должна молча подменять авторизацию SDK. Значения
		// проверяются наравне со своими: requestHeaderFields склеивает пары через
		// «; » без экранирования, поэтому ';' внутри чужого значения протаскивал
		// бы в заголовок произвольные пары — включая повторный sessionid, который
		// last-wins сервер и примет, то есть ровно в обход фильтра по имени.
		// Одноимённые упорядочены по возрастанию специфичности, побеждает
		// последняя. Порядок тотальный — sort в Swift нестабилен
		let storedCookies = (handshakeCookieStorage?.cookies(for: Self.cookieMatchURL(for: url)) ?? [])
			.filter { Self.reservedCookieNames.contains($0.name.lowercased()) == false }
			.filter { cookie in
				guard Self.isSerializableCookie(name: cookie.name, value: cookie.value) else {
					// Единственный отбрасывающий фильтр в addCookies без лога —
					// иначе кука хост-приложения пропадает бесследно
					Logger.assistant.error(S("Stored cookie dropped, not serializable: \(Self.logSafe(cookie.name))"))
					return false
				}
				return true
			}
			.sorted { Self.specificity(of: $0) < Self.specificity(of: $1) }
		for cookie in storedCookies {
			merged[cookie.name] = cookie
		}

		// Один await на все зарезервированные куки. Стораж — актор, но он
		// реентерабельный: если getCookie внутри сам ждёт (кейчейн, IPC),
		// ожидания перекрываются. Плюс одна точка приостановки вместо N —
		// каждая расширяет окно, в котором конкурентный establish/cancel
		// обесценит уже собранный запрос. Обход идёт по allCases, а не по
		// результату, чтобы порядок не зависел от того, кто финишировал первым
		let fetched = await fetchReservedCookies()
		for name in Cookies.allCases.map(\.rawValue) {
			guard let value = fetched[name] else {
				// Рукопожатие уйдёт без авторизации, и сервер закроет его сам —
				// без этого лога причина отказа неотличима от серверной
				Logger.assistant.error(S("Missing SDK cookie: \(name)"))
				continue
			}
			if let outCookie = createCookie(name: name, value: value, for: url) {
				merged[outCookie.name] = outCookie
			}
		}

		guard merged.isEmpty == false else { return }

		let sortedCookies = merged.values.sorted { $0.name < $1.name }
		let cookieHeaders = HTTPCookie.requestHeaderFields(with: sortedCookies)

		for (headerField, headerValue) in cookieHeaders {
			request.setValue(headerValue, forHTTPHeaderField: headerField)
		}
	}

	/// Сбор кук — единственная фаза до появления стрима: отчитаться о проблеме
	/// ещё не через что, и незавершившийся стораж подвесил бы сам
	/// `establishStream` (кейчейн на заблокированном устройстве — реальный
	/// сценарий). Поэтому фаза ограничена своим дедлайном, а гонка идёт через
	/// сигнальный стрим, а не через группу: группа дожидается ВСЕХ детей и
	/// зависшего чтения не бросила бы. По дедлайну рукопожатие уходит без кук —
	/// сервер его отклонит, и это определённый исход, а не вечное ожидание
	private func fetchReservedCookies() async -> [String: String] {
		let storage = cookieStorage
		let names = Cookies.allCases.map(\.rawValue)
		// Каждое чтение публикуется своим сообщением, а не общим результатом:
		// иначе одна зависшая кука уносила бы по дедлайну и те, что уже
		// прочитались, и рукопожатие уходило бы вовсе без авторизации
		let (signal, signalContinuation) = AsyncStream<ReservedCookieRead>.makeStream()

		let fetches = names.map { name in
			Task {
				let value = await storage.getCookie(name: name)
				signalContinuation.yield(ReservedCookieRead(name: name, value: value))
			}
		}
		// Дедлайн закрывает сигнальный стрим — цикл заканчивается тем, что
		// успело прийти, а недочитанное видно по остатку счётчика
		let deadline = Task {
			try? await Task.sleep(nanoseconds: Self.cookieCollectionNanoseconds)
			signalContinuation.finish()
		}
		defer {
			fetches.forEach { $0.cancel() }
			deadline.cancel()
			signalContinuation.finish()
		}

		var fetched = [String: String]()
		var pending = names.count
		for await item in signal {
			if let value = item.value { fetched[item.name] = value }
			pending -= 1
			if pending == 0 { break }
		}
		if pending > 0 {
			Logger.assistant.error(S("Cookie collection timed out after \(fetched.count)/\(names.count)"))
		}
		return fetched
	}

	/// Специфичность куки по RFC 6265 5.4: сперва длина path, при равной —
	/// host-only конкретнее доменной. Длину домена как признак брать нельзя:
	/// Foundation ставит доменной куке ведущую точку, и та ОКАЗЫВАЕТСЯ длиннее
	/// host-only варианта того же хоста
	private static func specificity(of cookie: HTTPCookie) -> (Int, Int, String, String) {
		(cookie.path.count, cookie.domain.hasPrefix(".") ? 0 : 1, cookie.path, cookie.domain)
	}

	/// ws→http, wss→https: матчинг стоража и secure-куки работают по HTTP-схемам
	static func cookieMatchURL(for url: URL) -> URL {
		guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return url }
		switch components.scheme?.lowercased() {
		case "ws": components.scheme = "http"
		case "wss": components.scheme = "https"
		default: break
		}
		return components.url ?? url
	}

	func createCookie(name: String, value: String, for url: URL) -> HTTPCookie? {
		guard Self.isSerializableCookie(name: name, value: value) else {
			Logger.assistant.error(S("Cookie rejected for \(Self.logSafe(name)): control characters or separators"))
			return nil
		}
		let cookie = HTTPCookie(properties: [
			.path: "/",
			.name: name,
			.value: value,
			.domain: url.host ?? ""
		])
		if cookie == nil {
			// Иначе рукопожатие уходит без авторизации, а в логе — ничего:
			// отказ сервера не отличить от настоящего серверного
			Logger.assistant.error(S("Cookie properties rejected by Foundation for \(Self.logSafe(name))"))
		}
		return cookie
	}

	/// Имя — RFC 7230 token (RFC 6265 4.1.1 ссылается на него же). Строгость
	/// тут не косметическая: имя вида " sessionid" прошло бы фильтр
	/// зарезервированных (сравнение точное), а сервер, срезающий пробелы,
	/// прочитал бы его как sessionid и получил вторую пару с тем же именем.
	///
	/// Про значение гарантия ровно одна: оно не разорвёт заголовок, который мы
	/// собираем, — отсечены CTL (C0, C1 и DEL) и разделители Cookie. Это НЕ
	/// проверка на cookie-octet из RFC 6265: пробел, кавычка, обратный слеш и
	/// не-ASCII проходят, и строгий сервер вправе такую пару отбросить.
	/// Ужесточать до allowlist нельзя вслепую — отрезало бы легальные форматы
	/// токенов. Cf (U+FEFF, U+200B, мягкий перенос) разрешены намеренно:
	/// заголовок ими не разорвать. '=' в значении легален и обязателен для
	/// base64 с паддингом — сервер режет пару по ПЕРВОМУ '='
	private static func isSerializableCookie(name: String, value: String) -> Bool {
		isToken(name) && isCookieOctets(value)
	}

	private static func isCookieOctets(_ text: String) -> Bool {
		text.unicodeScalars.allSatisfy { scalar in
			isPrintableScalar(scalar) && scalar != ";" && scalar != ","
		}
	}

	/// Управляющий символ, попавший на провод, ломает разбор у любого
	/// снисходительного парсера, поэтому правило одно на все тексты, которые
	/// мы туда кладём: ни C0, ни DEL, ни C1 (0x80–0x9F). Cf (U+FEFF, U+200B,
	/// мягкий перенос) разрешены намеренно — ими ничего не разорвать
	private static func isPrintableScalar(_ scalar: Unicode.Scalar) -> Bool {
		scalar.value >= 0x20 && scalar.value != 0x7F
			&& (scalar.value < 0x80 || scalar.value > 0x9F)
	}

	// MARK: - Header Validation

	/// Заголовки рукопожатия принадлежат транспорту: Foundation на Darwin
	/// вычищает их из запроса, а Sec-WebSocket-* формирует CFNetwork сам.
	/// Чужое значение всё равно не доедет — отбраковываем явно, иначе
	/// потеря была бы молчаливой (на Linux такая пара доехала бы и сломала
	/// рукопожатие: там Foundation её сохраняет)
	private static let reservedHeaderNames: Set<String> = [
		"connection", "upgrade", "host", "content-length", "transfer-encoding",
		// Cookie принадлежит addCookies: он всё равно перезапишет заголовок,
		// и без этой строки потеря была бы молчаливой
		"cookie"
	]

	private static func isReservedHeaderName(_ name: String) -> Bool {
		let lowered = name.lowercased()
		return reservedHeaderNames.contains(lowered) || lowered.hasPrefix("sec-websocket-")
	}

	/// RFC 7230 token — общий словарь для имён заголовков и кук. Имя с
	/// управляющими символами Foundation принимает дословно, поэтому
	/// отбраковываем сами. Пробел и таб уже отсечены проверкой `> 0x20`,
	/// в наборе разделителей их нет
	private static let tokenSeparators = Set("()<>@,;:\\\"/[]?={}".unicodeScalars)

	private static func isToken(_ text: String) -> Bool {
		guard text.isEmpty == false else { return false }
		return text.unicodeScalars.allSatisfy { scalar in
			scalar.isASCII && scalar.value > 0x20 && scalar.value != 0x7F
				&& tokenSeparators.contains(scalar) == false
		}
	}

	/// Значение с CR/LF/NUL Foundation отбрасывает вместе со всем заголовком —
	/// молча. HTAB легален и разрешён отдельно, остальное — общее правило
	private static func isValidHeaderValue(_ value: String) -> Bool {
		value.unicodeScalars.allSatisfy { scalar in
			scalar.value == 0x09 || isPrintableScalar(scalar)
		}
	}

	/// Всё, что лог-сток может принять за конец строки, схлопывается в '?':
	/// иначе чужое имя подделало бы вторую запись в логе. Кроме C0 и DEL это
	/// NEL и разделители строк/абзацев Unicode — их режут splitlines(),
	/// enumerateLines и типовые JS-сплиттеры. Длина режется до 64 символов:
	/// имя в логе нужно для опознания, а не целиком
	private static let logLineBreakers: Set<UInt32> = [0x85, 0x2028, 0x2029]

	private static func logSafe(_ text: String) -> String {
		let scrubbed = String(String.UnicodeScalarView(text.unicodeScalars.map { scalar in
			scalar.value < 0x20 || scalar.value == 0x7F || logLineBreakers.contains(scalar.value)
				? "?"
				: scalar
		}))
		return scrubbed.count > 64 ? String(scrubbed.prefix(64)) + "…" : scrubbed
	}
}

// MARK: - NetworkStreamingError

fileprivate extension NetworkStreamingError {

	/// Маппинг транспортной ошибки в кейс контракта.
	/// Схлопывание -1005 в `.timeout` — унаследованный контракт SDK: обрыв и
	/// молчащий сокет для потребителя означают одно и то же (переподнять
	/// соединение), тогда как `.nsError` он трактует как отказ, который ретраем
	/// не лечится. Мапинг лоссовый, поэтому `.timeout` в доке establishStream
	/// описан двумя значениями; менять только синхронно с вызывающим кодом
	static func transportFailure(_ error: NSError) -> NetworkStreamingError {
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorNetworkConnectionLost {
			return .timeout
		}
		return .nsError(error)
	}
}

// MARK: - WebSocketNetworkStreamingDelegate

final class WebSocketNetworkStreamingDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {

	/// Origin нормализуется один раз: он неизменен всё время жизни делегата,
	/// а цепочка редиректов дёргала бы разбор на каждом хопе
	private let origin: (scheme: String, host: String, port: Int?)?
	private let continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation

	init(origin: URL, continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation) {
		self.origin = Self.normalizedOrigin(of: origin)
		self.continuation = continuation
	}

	func urlSession(
		_ session: URLSession,
		webSocketTask: URLSessionWebSocketTask,
		didOpenWithProtocol protocol: String?
	) {
		continuation.yield(.connected)
	}

	func urlSession(
		_ session: URLSession,
		webSocketTask: URLSessionWebSocketTask,
		didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
		reason: Data?
	) {
		finish(closeCode: closeCode)
	}

	/// Единственная точка решения «код закрытия → исход», общая для обоих
	/// путей: close-фрейм от сервера и код, снятый с задачи в didComplete
	private func finish(closeCode: URLSessionWebSocketTask.CloseCode) {
		guard Self.isCleanClosure(closeCode) else {
			continuation.finish(
				throwing: NetworkStreamingError.closeCode(NetworkStreamingOutputError(code: closeCode))
			)
			return
		}
		continuation.finish()
	}

	/// Сбоем не считаются: 1000, 1005 («кода не было» — легальный исход по
	/// RFC 6455 §7.1.5) и .invalid — сентинел «код не записан», по которому
	/// сказать о сбое нечего
	static func isCleanClosure(_ closeCode: URLSessionWebSocketTask.CloseCode) -> Bool {
		closeCode == .normalClosure || closeCode == .noStatusReceived || closeCode == .invalid
	}
}

// MARK: - URLSessionTaskDelegate

extension WebSocketNetworkStreamingDelegate: URLSessionTaskDelegate {

	/// Куки прикреплены к запросу статическим заголовком, поэтому per-host
	/// скоупинг URLSession здесь не работает: редирект на чужой origin унёс бы
	/// авторизацию SDK на хост, для которого она не выдавалась. Свой origin
	/// пропускаем — это обычная смена пути
	func urlSession(
		_ session: URLSession,
		task: URLSessionTask,
		willPerformHTTPRedirection response: HTTPURLResponse,
		newRequest request: URLRequest,
		completionHandler: @Sendable @escaping (URLRequest?) -> Void
	) {
		guard let origin,
			  let target = request.url.flatMap(Self.normalizedOrigin(of:)),
			  origin == target else {
			Logger.assistant.error(S("WebSocket handshake redirect refused"))
			// Причина фиксируется ДО возврата управления (как в failStream):
			// отказ завершает задачу без ошибки, и didComplete, успей он первым,
			// выдал бы стрим за штатно закрытый — finish первый-выигрывает
			continuation.finish(throwing: NetworkStreamingError.nsError(URLError(.unsupportedURL) as NSError))
			completionHandler(nil)
			return
		}
		completionHandler(request)
	}

	func urlSession(
		_ session: URLSession,
		task: URLSessionTask,
		didCompleteWithError error: Error?
	) {
		// Состоявшееся close-рукопожатие важнее ошибки завершения: код известен,
		// а ошибка после него — шум нашего же teardown'а. .invalid значит, что
		// рукопожатия не было и решает ошибка. Это же спасает код, когда
		// didCloseWith не доехал (замерено: теряется безотносительно кода)
		if let webSocketTask = task as? URLSessionWebSocketTask,
		   webSocketTask.closeCode != .invalid {
			finish(closeCode: webSocketTask.closeCode)
			return
		}
		guard let error = error as NSError? else {
			continuation.finish()
			return
		}
		// Наше собственное закрытие (в т.ч. отмена задач в deinit) и POSIX 57 —
		// финал гонки с close-фреймом: для потребителя это не ошибка
		guard Self.isCleanTransportError(error) == false else {
			continuation.finish()
			return
		}
		// Унаследованный контракт SDK: потребители различают исходы по этим
		// кейсам, менять только синхронно с вызывающим кодом
		continuation.finish(throwing: NetworkStreamingError.transportFailure(error))
	}

	/// Схема приводится к HTTP-виду (ws→http, wss→https): рукопожатие и его
	/// редиректы живут в HTTP-схемах, порт по умолчанию берётся оттуда же
	private static func normalizedOrigin(of url: URL) -> (scheme: String, host: String, port: Int?)? {
		let normalized = WebSocketNetworkStreaming.cookieMatchURL(for: url)
		guard let scheme = normalized.scheme?.lowercased(),
			  let host = normalized.host.map(WebSocketNetworkStreaming.canonicalHost) else {
			return nil
		}
		let port: Int?
		switch (normalized.port, scheme) {
		case (let explicit?, _): port = explicit
		case (nil, "https"): port = 443
		case (nil, "http"): port = 80
		default: port = nil
		}
		return (scheme, host, port)
	}

	static func isCleanTransportError(_ error: NSError) -> Bool {
		(error.domain == NSURLErrorDomain && error.code == NSURLErrorCancelled)
			|| (error.domain == NSPOSIXErrorDomain && error.code == Int(POSIXErrorCode.ENOTCONN.rawValue))
	}
}
