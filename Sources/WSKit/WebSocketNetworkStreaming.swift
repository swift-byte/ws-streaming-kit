import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

/// Что делегат сообщает connectTask'у по ходу жизни соединения. Отдельный тип,
/// а не `NetworkStreamingOutputEvent`: наружу и внутрь ездит разное, и общий
/// тип заставлял бы гадать, какие его случаи здесь вообще возможны
enum HandshakeEvent: Sendable {
	case connected
}

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
	/// Общий порядок обхода зарезервированных кук: сбор и слияние должны
	/// ходить по одному списку, а не строить его каждый своим способом
	private static let reservedCookieOrder = Cookies.allCases.map(\.rawValue)

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
	/// Политика буфера объявлена рядом с глубиной, потому что байтовый счёт
	/// снимается с `remaining` и верен только для ограниченной политики:
	/// у `.unbounded` remaining равен Int.max, глубина схлопнулась бы в ноль,
	/// и байтовая граница молча перестала бы существовать
	private static let outputBufferingPolicy = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>
		.Continuation.BufferingPolicy.bufferingOldest(outputBufferDepth)

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
		// Close-фрейма мы больше не шлём (см. cancel(generation:)), так что
		// ждать его отправки незачем: invalidateAndCancel сразу отпускает
		// сессию и её делегата
		session.invalidateAndCancel()
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
	/// - Сбой отправки во входном стриме пишется в лог (домен и код), но СВОИМ
	///   исходом не становится — почему именно, см. `noteUplinkFailure`.
	///   Соединение при этом не закрывается: транспорт уже кончился и назовёт
	///   причину сам. Исключение — сбой отправки терминатора: там сервер иначе
	///   ждал бы ввод вечно, и сокет закрываем мы.
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

		// Отмена проверяется ДО вытеснения: иначе отменённый вызов сносил бы
		// действующий стрим (его потребитель получил бы CancellationError,
		// то есть «тебя заменили»), а замены бы не появилось — обе стороны
		// решили бы, что соединением владеет другая
		if Task.isCancelled { throw CancellationError() }

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
			.makeStream(bufferingPolicy: Self.outputBufferingPolicy)

		// Потребитель бросил стрим — значит, соединение больше некому читать:
		// закрываем его. Поколение в захвате делает хук безвредным для всех
		// последующих стримов
		outputContinuation.onTermination = { [weak self] _ in
			Task { await self?.cancel(generation: currentGeneration) }
		}

		var request = URLRequest(url: url)
		// У самого запроса дефолт 60с, и он перекрывает конфигурацию сессии —
		// без этой строки платформа опережала бы наш дедлайн при timeout > 30
		request.timeoutInterval = Self.platformRequestTimeout(for: timeout)
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

		// Сбор кук вычитывает сигнальный стрим, а тот под отменой отдаёт nil
		// сразу — на отменённой задаче куки не соберутся. Открывать в этом
		// случае анонимный сокет (сервер его отклонит, и это будет выглядеть
		// провалом авторизации) хуже, чем честно вернуть отмену
		if Task.isCancelled {
			outputContinuation.onTermination = nil
			outputContinuation.finish()
			cancel(generation: currentGeneration)
			throw CancellationError()
		}

		guard generation == currentGeneration else {
			// Пока собирали куки, поколение сдвинулось. Причину различаем по
			// токену: вытеснение — CancellationError (ретрай-логика не должна
			// путать замену со штатным закрытием), явная отмена — тихий finish,
			// единый с контрактом поднятого стрима
			outputContinuation.onTermination = nil
			outputContinuation.finish(throwing: outcome(for: lifecycleToken, terminalError: nil))
			return outputStream
		}

		let (delegateStream, delegateContinuation) = AsyncThrowingStream<HandshakeEvent, Error>.makeStream()

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

	/// Снимает дедлайн ожидания первого входящего сообщения у ТЕКУЩЕГО стрима.
	/// Идентичности у стримов нет, как и у `cancel()`: вызов, разошедшийся с
	/// новым `establishStream`, снимет страховку уже с нового стрима, и тот
	/// будет ждать первого сообщения до платформенного дедлайна простоя.
	/// Дедлайн тикает и после `.connected`, так что снять его — единственный
	/// способ пережить долгую паузу в диалоге.
	///
	/// Платформенный дедлайн простоя при этом ОСТАЁТСЯ и снять его нечем:
	/// соединение, молчащее дольше `platformRequestTimeout(for:)`, CFNetwork
	/// закроет сам, и потребитель увидит `.nsError` (NSURLErrorTimedOut).
	/// Это осознанный размен: без него соединение, умершее без FIN, висело бы
	/// вечно. Приложению, которому нужны более долгие паузы, придётся греть
	/// соединение своим трафиком либо поднять `timeout` при инициализации.
	///
	/// Снятие относится к стриму, который существует В МОМЕНТ вызова: следующий
	/// `establishStream` начинается со взведённой страховкой, и снимать её надо
	/// заново. Вызов, попавший в окно сбора кук поднимающегося стрима, достаётся
	/// уже ему — и тот остаётся вообще без страховки: зависшее рукопожатие
	/// продержится не `timeout`, а до платформенного дедлайна простоя.
	/// Стримы не различимы по имени (как и в `cancel()`), поэтому упорядочивать
	/// снятие и переподъём — забота вызывающего.
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
		// Только каноническая запись из четырёх десятичных октетов без ведущих
		// нулей. Резолвер понимает и 127.1, и 0x7f.1, и 2130706433, и октет с
		// ведущим нулём читает ВОСЬМЕРИЧНО — «0127.1» у него 87.0.0.1, адрес
		// публичный (замерено). Повторять inet_aton здесь нельзя: любое
		// расхождение — это плейнтекст с куками SDK не туда, куда мы решили.
		// Нестандартную запись отвергаем; развернуть её до 127.0.0.1 — дело
		// вызывающего, а осознанный плейнтекст открывается allowsInsecureEndpoint
		let octets = canonical.split(separator: ".", omittingEmptySubsequences: false)
		guard octets.count == 4, octets.allSatisfy(Self.isPlainDecimalOctet) else { return false }
		return octets[0] == "127"
	}

	private static func isPlainDecimalOctet(_ text: Substring) -> Bool {
		guard (1...3).contains(text.count),
			  text.allSatisfy(\.isASCII),
			  text.allSatisfy(\.isNumber),
			  text == "0" || text.hasPrefix("0") == false,
			  UInt8(text) != nil else {
			return false
		}
		return true
	}

	// MARK: - Private Methods

	// Тело обязано оставаться синхронным (без await): establishStream
	// полагается на неразрывность «пометка токена → teardown → чтение
	// поколения» внутри своей изоляции.
	private func cancel(generation requested: Int) {
		guard requested == generation else { return }
		// Отмена ввода — до разрыва сокета: inflight-send иначе падал от
		// разрыва раньше, чем задача видела отмену, и штатный teardown
		// эскалировался как сбой аплинка
		inputTask?.cancel()
		inputTask = nil
		// Штатное закрытие с кодом: сервер должен отличать «пользователь
		// договорил» от «связь оборвалась». Своим словом сервера этот код уже
		// не притворится — completionOutcome доверяет только кодам С ПРИЧИНОЙ,
		// а чистый разбирает наравне с ошибкой завершения
		webSocketTask?.cancel(with: .normalClosure, reason: nil)
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
		// .terminated значит, что потребитель бросил стрим, а хоп его
		// onTermination ещё не долетел до актора. Продолжать нельзя: иначе
		// поднимутся читатель и аплинк ради стрима, который никто не читает,
		// и в сокет полетит ввод вызывающего
		if case .terminated = outputContinuation.yield(.connected) { return false }
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
	/// Вытеснение перебивает и уже наступившее штатное закрытие: пока стрим не
	/// финишировал, он остаётся вытесняемым, и подменившему нужен однозначный
	/// CancellationError. Обратный порядок сделал бы контракт вытеснения
	/// зависимым от того, успел ли сервер попрощаться в окне дренажа
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
		delegateContinuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation
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
	/// Снятый onTermination убирает Task с отменой, который гонялся бы с нашим
	/// teardown
	private func failStream(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		delegateContinuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation,
		error: NetworkStreamingError
	) {
		guard requested == generation else { return }
		outputContinuation.onTermination = nil
		outputContinuation.finish(throwing: error)
		delegateContinuation.finish(throwing: error)
		cancel(generation: requested)
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
			/// Проверка отсекает штатный teardown: сокет рвётся раньше, чем send
			/// видит отмену, и это не сбой аплинка
			func noteSendFailure(_ error: Error, closing: Bool) async {
				guard Task.isCancelled == false else { return }
				await self?.noteUplinkFailure(generation: requested, error: error as NSError, closing: closing)
			}

			var sendFailed = false
			for await data in inputStream {
				if sendFailed { continue } // дожигаем ввод: буфер продюсера не растёт
				do {
					try await task.send(.data(data))
				} catch {
					sendFailed = true
					// Сокет НЕ закрываем: send падает, когда транспорт уже
					// кончился, и его настоящую причину сейчас записывает
					// CFNetwork. Наша отмена, успей она первой, подменила бы её
					// на -999, а тот делегат считает чистым завершением —
					// реальный обрыв связи стал бы тихим финишем
					await noteSendFailure(error, closing: false)
				}
			}
			// Терминатор шлём и после сбоя: если транспорт всё-таки жив, сервер
			// дождётся конца ввода и ответит; если нет — отправка упадёт и мы
			// закроем сокет. Пропустить его значило бы оставить сервер ждать
			// аплинк, а потребителя — висеть до платформенного дедлайна
			guard Task.isCancelled == false else { return }
			Logger.assistant.info(S("Input stream finished"))
			do {
				try await task.send(.data(Self.inputTerminator))
			} catch {
				// А здесь закрываем: без терминатора сервер продолжает добирать
				// аплинк и может не ответить вовсе. Ждать тут нечего, поэтому
				// риск подменить причину оправдан
				await noteSendFailure(error, closing: true)
				guard Task.isCancelled == false else { return }
				task.cancel()
			}
		}
	}

	/// Сбой отправки остаётся в логе и НЕ становится исходом стрима. Это
	/// решение, а не упущение: сделать его исходом пробовали трижды, и каждый
	/// раз замеры показывали, что достоверно приписать его некуда.
	/// 1. Собственный finish на общей continuation выигрывает гонку у
	///    didCloseWith: с активным вводом /close1011 отдавал .timeout вместо
	///    closeCode(1011), а штатный /close1000 — .timeout вместо тихого
	///    финиша (6/6 и 8/8 прогонов).
	/// 2. Отложить и достать по коду закрытия нельзя: платформа выставляет код
	///    и там, где close-фрейма не было (corelibs на обрыве TCP отдаёт 1000).
	/// 3. Отложить и достать по факту прихода close-фрейма (didCloseWith)
	///    тоже нельзя: send падает ПОЗЖЕ, чем делегат называет исход, и
	///    connectTask успевает финишировать раньше, чем сбой записан.
	/// Причина в природе события: send падает лишь тогда, когда транспорт уже
	/// кончился, а исход транспорта называет делегат — он один видит close-код.
	/// Цена: транспорт, завершившийся чисто, оставляет обрыв аплинка только в
	/// логе. Убрать её внутри этого файла нечем — у `NetworkStreamingOutputEvent`
	/// просто нет случая «даунлинк жив, аплинк умер», и любой обходной путь
	/// сводится к трём выше. Настоящее решение — отдельный случай в контракте
	/// SDK, и вводить его надо синхронно с вызывающим кодом.
	/// Страховку снимаем — соединение уже рвётся, и таймаут, выстрелив в окне
	/// дренажа, подменил бы причину
	/// - Parameter closing: закрываем ли мы сокет вслед за этим. Страховку
	///   снимаем только тогда: иначе таймаут гонялся бы с нашим же teardown.
	///   Оставлять её взведённой при `closing: false` смысла немного — первое
	///   же принятое сообщение её уже сняло, — но и снимать нечего: стрим здесь
	///   ограничивает не она, а отправка терминатора, которая после конца ввода
	///   либо доедет, либо закроет сокет
	private func noteUplinkFailure(generation requested: Int, error: NSError, closing: Bool) {
		guard requested == generation else { return }
		Logger.assistant.error(S("WebSocket input send failed: \(error.domain) \(error.code)"))
		if closing { disarmTimeout() }
	}

	/// Заводит читателя и отдаёт сигнал его завершения; nil — поколение сдвинулось.
	private func createOutputTask(
		generation requested: Int,
		task: URLSessionWebSocketTask,
		delegateContinuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> AsyncStream<Void>? {
		// Хоп отменённой connectTask всё равно исполняется: без guard'а поздний
		// вызов затёр бы outputTask нового поколения — тот остался бы живым,
		// но без ссылки, и teardown его уже не отменил бы
		guard requested == generation else { return nil }
		let (done, doneContinuation) = AsyncStream<Void>.makeStream()
		// detached по той же причине, что и inputTask: Task {} внутри актора
		// наследует его изоляцию, и весь покадровый разбор — switch, ledger,
		// yield — исполнялся бы на сериальном экзекьюторе актора, конкурируя
		// с cancel(), invalidate() и сбором кук следующего стрима. Изоляция
		// нужна ровно двум вызовам ниже, и они берут её сами
		let reader = Task.detached { [weak self] in
			// Сигнал завершения — от самого читателя: ожидание в drainOutput
			// становится отменяемым, промежуточный waiter не нужен
			defer { doneContinuation.finish() }
			var isFirstMessage = true
			var warnedTextFrame = false
			var warnedUnknownFrame = false
			var warnedUnknownYield = false
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
						// remaining клампится до вычитания: у неограниченной
						// политики он Int.max, и разность переполнилась бы
						// раньше, чем её увидел бы ledger
						let queuedBytes = ledger.record(
							size: payload.count,
							depth: Self.outputBufferDepth - min(remaining, Self.outputBufferDepth)
						)
						guard queuedBytes > Self.outputBufferBytes else { continue }
					case .dropped:
						break
					case .terminated:
						// Стрим уже завершён кем-то ещё — читать больше некому
						return
					@unknown default:
						// Доставлен кадр или нет — неизвестно. Рвать рабочий
						// стрим по неизвестному случаю хуже, чем продолжить:
						// диагноз «переполнение» был бы выдуманным. Молчать
						// тоже нельзя — логируем, один раз.
						// В вес очереди кадр не идёт: глубину даёт только
						// `.enqueued`, а выдумывать её — портить учёт
						if warnedUnknownYield == false {
							warnedUnknownYield = true
							Logger.assistant.error(S("Unknown yield result, frame delivery unverified"))
						}
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
		delegateStream: AsyncThrowingStream<HandshakeEvent, Error>,
		delegateContinuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation,
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
						// self связывается на время настройки — окно короткое и
						// заканчивается вместе с этим блоком, актор оно не удержит
						guard let self, await self.yieldConnectedIfCurrent(
							generation: requested,
							outputContinuation: outputContinuation
						) else {
							continue
						}
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
			// ?? terminalError схлопывает двойную опциональность: актор мог уйти
			// за время дренажа, и тогда спросить его не о чем — но уже
			// наблюдённая причина от этого не перестаёт быть причиной
			let outcome = await self?.outcome(
				for: lifecycleToken,
				terminalError: terminalError
			) ?? terminalError
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

		// Слияние вместо замещения: чужие куки сохраняются, зарезервированные
		// имена из стоража не берутся вовсе (см. фильтр ниже — именно он, а не
		// порядок слияния, не даёт подменить авторизацию). Ключ — имя, без
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
			.filter { cookie in
				// Своя кука с зарезервированным именем всё равно перекрыла бы
				// чужую, но молча потерянная кука хост-приложения — повод для
				// вопросов, поэтому оба отказа логируются
				guard Self.reservedCookieNames.contains(cookie.name.lowercased()) == false else {
					Logger.assistant.error(S("Stored cookie dropped, name reserved: \(Self.logSafe(cookie.name))"))
					return false
				}
				guard Self.isSerializableCookie(name: cookie.name, value: cookie.value) else {
					Logger.assistant.error(S("Stored cookie dropped, not serializable: \(Self.logSafe(cookie.name))"))
					return false
				}
				return true
			}
			.map { (key: Self.specificity(of: $0), cookie: $0) }
			.sorted { $0.key < $1.key }
			.map(\.cookie)
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
		for name in Self.reservedCookieOrder {
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
		let names = Self.reservedCookieOrder
		guard names.isEmpty == false else { return [:] }
		// Каждое чтение публикуется своим сообщением, а не общим результатом:
		// иначе одна зависшая кука уносила бы по дедлайну и те, что уже
		// прочитались, и рукопожатие уходило бы вовсе без авторизации
		let (signal, signalContinuation) = AsyncStream<ReservedCookieRead>.makeStream()

		// detached по той же причине, что inputTask и outputTask: Task {} внутри
		// актора наследует изоляцию и держит его СИЛЬНО, а зависшее чтение
		// (кейчейн на заблокированном устройстве — тот самый сценарий, ради
		// которого тут дедлайн) тогда не давало бы актору деинициализироваться,
		// и сокет с сессией текли бы до конца процесса
		let fetches = names.map { name in
			Task.detached {
				let value = await storage.getCookie(name: name)
				signalContinuation.yield(ReservedCookieRead(name: name, value: value))
			}
		}
		// Дедлайн закрывает сигнальный стрим — цикл заканчивается тем, что
		// успело прийти, а недочитанное видно по остатку счётчика
		let deadline = Task.detached {
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
			// Отмена вызывающего и истёкший дедлайн выглядят одинаково — стрим
			// в обоих случаях отдаёт nil, — но лечатся по-разному, поэтому
			// различаем. Числитель считает ЗАВЕРШИВШИЕСЯ чтения: вернуть nil —
			// тоже результат
			let completed = names.count - pending
			if Task.isCancelled {
				Logger.assistant.info(S("Cookie collection cancelled after \(completed)/\(names.count)"))
			} else {
				Logger.assistant.error(S("Cookie collection timed out after \(completed)/\(names.count)"))
			}
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
		// Имя и значение отбраковываются по разным правилам, поэтому и в логе
		// разделены: иначе диагностика вела бы не к той половине пары
		guard Self.isToken(name) else {
			Logger.assistant.error(S("Cookie rejected, name is not a token: \(Self.logSafe(name))"))
			return nil
		}
		guard Self.isCookieOctets(value) else {
			Logger.assistant.error(S("Cookie rejected, value has control characters or separators: \(Self.logSafe(name))"))
			return nil
		}
		let cookie = HTTPCookie(properties: [
			.path: "/",
			.name: name,
			.value: value,
			.domain: url.host.map(Self.canonicalHost) ?? ""
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
	/// иначе чужое имя подделало бы вторую запись в логе. Правило то же, что и
	/// для провода (`isPrintableScalar`, туда входит и NEL из C1), плюс
	/// разделители строк и абзацев Unicode — их режут splitlines(),
	/// enumerateLines и типовые JS-сплиттеры. Длина режется до 64 символов:
	/// имя в логе нужно для опознания, а не целиком
	private static let logLineBreakers: Set<UInt32> = [0x2028, 0x2029]

	private static func logSafe(_ text: String) -> String {
		let scrubbed = String(String.UnicodeScalarView(text.unicodeScalars.map { scalar in
			isPrintableScalar(scalar) == false || logLineBreakers.contains(scalar.value)
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
	private let continuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation

	init(origin: URL, continuation: AsyncThrowingStream<HandshakeEvent, Error>.Continuation) {
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
		// Локальные сентинелы (1006/1015) сервер прислать не мог: настоящую
		// причину знает didCompleteWithError, и она приедет следом. Молчим,
		// иначе тот же обрыв давал бы .closeCode или .timeout по гонке
		guard Self.isPeerCloseCode(closeCode) else { return }
		continuation.finish(throwing: Self.closeFrameOutcome(closeCode))
	}

	/// Сбоем не считаются 1000 и 1005. Про 1005 стоит помнить, что сам код в
	/// close-фрейме запрещён (RFC 6455 §7.4.1) — его ставит принимающая
	/// сторона, когда фрейм пришёл БЕЗ кода; закрытие при этом состоялось,
	/// поэтому сбоем оно не считается. Это осознанное отличие от исходной
	/// версии, где 1005 приезжал ошибкой.
	/// Вызывается только после `isPeerCloseCode`, поэтому локальные сентинелы
	/// (включая .invalid) сюда не доходят и в перечислении не участвуют
	static func isCleanClosure(_ closeCode: URLSessionWebSocketTask.CloseCode) -> Bool {
		closeCode == .normalClosure || closeCode == .noStatusReceived
	}

	/// Код, который мог прийти в close-фрейме от сервера. 1006 и 1015 по
	/// RFC 6455 §7.4.1 в фрейме запрещены — их выставляет локальная сторона,
	/// когда фрейма не было вовсе, поэтому словом сервера они не являются и
	/// не должны перебивать ошибку завершения (для -1005 это означало бы
	/// подмену унаследованного `.timeout` на `.closeCode`)
	static func isPeerCloseCode(_ closeCode: URLSessionWebSocketTask.CloseCode) -> Bool {
		switch closeCode {
		case .invalid, .abnormalClosure, .tlsHandshakeFailure:
			return false
		default:
			return true
		}
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
		let closeCode = (task as? URLSessionWebSocketTask)?.closeCode ?? .invalid
		continuation.finish(throwing: Self.completionOutcome(closeCode: closeCode, error: error as NSError?))
	}

	/// Таблица исходов завершения задачи, вынесенная в чистую функцию: состояние
	/// закрытой задачи в тесте воспроизводится плохо, а решение проверять надо
	/// целиком. nil — чистый финиш.
	///
	/// Состоявшееся close-рукопожатие важнее ошибки завершения: код сказал
	/// сервер, а ошибка после него — шум нашего же teardown'а. Это же спасает
	/// код, когда didCloseWith не доехал (замерено: теряется безотносительно
	/// кода). Локальные сентинелы (см. isPeerCloseCode) сервер прислать не мог —
	/// там исход называет ошибка.
	/// Наше собственное закрытие (в т.ч. отмена задач в deinit) и POSIX 57 —
	/// финал гонки с close-фреймом: для потребителя это не ошибка.
	/// Всё прочее — унаследованный контракт SDK: потребители различают исходы
	/// по этим кейсам, менять только синхронно с вызывающим кодом
	static func completionOutcome(
		closeCode: URLSessionWebSocketTask.CloseCode,
		error: NSError?
	) -> NetworkStreamingError? {
		// Код с причиной перевешивает ошибку завершения: её сервер и объясняет.
		// А вот ЧИСТЫЙ код — нет: платформа выставляет его и там, где фрейма не
		// было (замерено: corelibs на обрыве TCP отдаёт 1000), и приняв его за
		// слово сервера мы проглотили бы настоящую транспортную ошибку.
		// Настоящее чистое закрытие приходит через didCloseWith и финиширует
		// раньше — сюда с ошибкой в паре оно не доезжает
		if isPeerCloseCode(closeCode), isCleanClosure(closeCode) == false {
			return closeFrameOutcome(closeCode)
		}
		if let error, isCleanTransportError(error) == false {
			return .transportFailure(error)
		}
		// Ошибки нет либо она наша. Но 1006/1015 — не «ничего не известно», а
		// «фрейма не было»: закрытие ненормальное, и молчать о нём нельзя
		guard closeCode == .abnormalClosure || closeCode == .tlsHandshakeFailure else { return nil }
		return .closeCode(NetworkStreamingOutputError(code: closeCode))
	}

	/// Исход по коду, пришедшему во фрейме: код и есть вся история
	static func closeFrameOutcome(_ closeCode: URLSessionWebSocketTask.CloseCode) -> NetworkStreamingError? {
		guard isCleanClosure(closeCode) == false else { return nil }
		return .closeCode(NetworkStreamingOutputError(code: closeCode))
	}

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
