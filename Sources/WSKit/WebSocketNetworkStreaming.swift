import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

actor WebSocketNetworkStreaming: NetworkStreaming {

	// MARK: - Private Types

	/// Причина разрушения стрима, проставленная его инициатором. Пишет тот, кто
	/// рвёт соединение; читает connectTask разрушаемого стрима, чтобы отличить
	/// вытеснение (CancellationError) от явной отмены (тихий finish).
	/// Оба поля читаются и пишутся только в изоляции актора; @unchecked нужен,
	/// чтобы токен можно было захватить замыканием connectTask
	private final class LifecycleToken: @unchecked Sendable {
		var superseded = false
		var cancelled = false
	}

	// MARK: - Private Properties

	private static let inputTerminator = Data([0x31])
	private static let reservedCookieNames = Set(Cookies.allCases.map(\.rawValue))

	/// Граница ожидания дренажа: по истечении стрим финиширует, не дожидаясь
	/// читателя. В happy path снимается сразу, как только читатель завершился
	private static let drainGraceNanoseconds: UInt64 = 3_000_000_000

	/// Потолок кадра. Совпадает с платформенным дефолтом и задаётся явно, чтобы
	/// верхняя граница памяти была видна в коде: кадр больше потолка закрывает
	/// соединение кодом 1009
	private static let maximumMessageSize = 1024 * 1024

	/// Глубина очереди выходного стрима. Контракт прежний — потребитель обязан
	/// вычитывать стрим, backpressure нет. Граница нужна лишь для того, чтобы
	/// залипший потребитель или заливающий сервер давали наблюдаемый отказ,
	/// а не безграничный рост (худший случай — outputBufferDepth × maximumMessageSize)
	private static let outputBufferDepth = 1024

	/// Платформенный дедлайн простоя — последняя страховка от соединения,
	/// умершего без FIN: сокет, молчащий дольше, CFNetwork закроет сам.
	/// Дефолтные 60с рвали здоровое соединение раньше нашего таймера и
	/// обесценивали invalidate(), поэтому дедлайн выводится из timeout и
	/// заведомо длиннее его. Приложению, которому нужны более долгие паузы,
	/// придётся греть соединение на своём уровне: ping/pong тут не заводим —
	/// проверить его на целевой платформе из этого окружения нечем
	private static func platformRequestTimeout(for timeout: Int) -> TimeInterval {
		max(TimeInterval(timeout) * 2, 300)
	}

	private let kidsURLSession: KidsURLSession
	private let cookieStorage: CookieStorage
	private let timeout: Int
	private let session: URLSession
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
	init(
		kidsURLSession: KidsURLSession,
		cookieStorage: CookieStorage,
		timeout: Int? = nil
	) {
		self.kidsURLSession = kidsURLSession
		self.cookieStorage = cookieStorage
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

	// Continuations финиширует connectTask (обе ветви do/catch); deinit ссылок
	// на них не имеет. Инвариант: внутри цикла connectTask нет ранних return
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
	/// - тихий `finish()` — штатное закрытие сервером или явный `cancel()`;
	/// - `CancellationError` — стрим вытеснен следующим `establishStream`;
	/// - `NetworkStreamingError.timeout` — первое сообщение не пришло за
	///   `timeout` секунд; дедлайн тикает и ПОСЛЕ `.connected`, снять его можно
	///   только через `invalidate()`;
	/// - `NetworkStreamingError.closeCode` / `.nsError` — закрытие с кодом или
	///   транспортная ошибка, включая переполнение очереди выходного стрима.
	///
	/// - Каждому вызову нужен свежий inputStream: AsyncStream одноразов и
	///   single-consumer, повторная передача делит данные недетерминированно.
	/// - До `.connected` inputStream не потребляется; при отказе на этой фазе он
	///   освобождается, продюсеру приходит onTermination.
	/// - Отказы валидации endpoint — единственные без teardown: действующий
	///   стрим сохраняется.
	/// - Заголовок с управляющими символами Foundation молча отбрасывает, поэтому
	///   такие пары не отправляются, а логируются.
	/// - Если `cookieStorage` не отдал зарезервированную куку, рукопожатие уйдёт
	///   без неё: сервер закроет соединение, потребитель увидит `.closeCode`.
	func establishStream(
		endpoint: String,
		headers: [String: String],
		inputStream: AsyncStream<Data>
	) async throws -> AsyncThrowingStream<AssistantSDK.NetworkStreamingOutputEvent, any Error> {
		let url = try Self.validate(endpoint: endpoint)

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
		let (delegateStream, delegateContinuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.makeStream()

		outputContinuation.onTermination = { [weak self] _ in
			Task { await self?.cancel(generation: currentGeneration) }
		}

		var request = URLRequest(url: url)
		for (key, value) in headers {
			guard Self.isValidHeaderName(key), Self.isValidHeaderValue(value) else {
				// Такую пару Foundation отбрасывает целиком и молча — без лога
				// заголовка просто не окажется на проводе, и это никак не видно.
				// Имя в лог идёт очищенным: CR/LF в нём подделали бы запись лога
				Logger.assistant.error(S("Header rejected: \(Self.logSafe(key))"))
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
			delegateContinuation.finish()
			return outputStream
		}

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

	/// Закрывает ТЕКУЩЕЕ соединение актора и отменяет его задачи; стрим
	/// завершается тихим `finish()`. Идентичности у стримов нет — сигнатура
	/// унаследована от протокола, — поэтому вызов, разошедшийся с новым
	/// `establishStream`, закроет уже новый стрим, и тот финиширует, не отдав
	/// ни одного события; вызывающему стоит упорядочивать отмену и переподъём.
	/// Хвост входящих при отмене НЕ гарантирован: тот же teardown отменяет
	/// читателя, и уже принятый, но ещё не отданный кадр теряется. Дозакрытие с
	/// доставкой хвоста гарантировано только для закрытия по инициативе сервера.
	func cancel() async {
		cancel(generation: generation)
	}

	// MARK: - Deadline Control

	/// Снимает дедлайн ожидания первого входящего сообщения. Дедлайн тикает и
	/// после `.connected`, так что это единственный способ держать долго
	/// молчащее соединение открытым; дальше дедлайнами управляет вызывающий.
	/// В протокол `NetworkStreaming` не входит — доступен по конкретному типу.
	func invalidate() {
		// Разоружение коммитится в том же изолированном шаге, что и отмена
		// хэндла: иначе оставалось окно в один хоп, где уже проснувшийся таймер
		// добегал до fireTimeout и рвал стрим после явного invalidate()
		isTimeoutArmed = false
		timer?.cancel()
		timer = nil
	}

	// MARK: - Endpoint Validation

	/// Отбраковывает endpoint до любого teardown'а. Internal, а не private,
	/// ради прямого теста без поднятия соединения.
	/// - `.badURL` — строка не разобралась либо в ней нет схемы или хоста.
	/// - `.unsupportedURL` — схему вебсокет-задача не умеет вести; иначе отказ
	///   пришёл бы поздно, уже разрушив действующий стрим.
	/// - `.appTransportSecurityRequiresSecureConnection` — транспорт без TLS вне
	///   loopback: SDK-куки ушли бы открытым текстом и достались бы любому на пути.
	static func validate(endpoint: String) throws -> URL {
		guard let url = URL(string: endpoint),
			  let scheme = url.scheme?.lowercased(),
			  let host = url.host,
			  host.isEmpty == false else {
			throw URLError(.badURL)
		}
		switch scheme {
		case "wss", "https":
			return url
		case "ws", "http":
			guard isLoopback(host: host) else {
				Logger.assistant.error(S("Insecure endpoint rejected: \(scheme)"))
				throw URLError(.appTransportSecurityRequiresSecureConnection)
			}
			return url
		default:
			throw URLError(.unsupportedURL)
		}
	}

	private static func isLoopback(host: String) -> Bool {
		switch host.lowercased() {
		case "127.0.0.1", "localhost", "::1", "[::1]":
			return true
		default:
			return false
		}
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
		// Причина фиксируется на токене до того, как он потеряется: connectTask
		// отличит явную отмену (тихий finish) от гонки с терминальным событием
		currentToken?.cancelled = true
		// Отмена ввода — до разрыва сокета: inflight-send иначе падал от
		// разрыва раньше, чем задача видела отмену, и штатный teardown
		// эскалировался как сбой аплинка
		inputTask?.cancel()
		inputTask = nil
		webSocketTask?.cancel(with: closeCode, reason: nil)
		webSocketTask = nil
		// Страховка живёт ровно столько же, сколько остальные слоты: иначе
		// унаследованное состояние блокировало бы invalidate() в окне кук
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

	/// Изоляционный шлюз: connectTask читает флаги токена снаружи изоляции,
	/// а дисциплина @unchecked требует все обращения вести через актора.
	private func outcome(for token: LifecycleToken) -> (superseded: Bool, cancelled: Bool) {
		(token.superseded, token.cancelled)
	}

	private func acceptFirstMessage(generation requested: Int) -> Bool {
		// Опоздавший байт отсекает поколение: fireTimeout двигает его в том же
		// изолированном шаге, что и решение о таймауте
		guard requested == generation else { return false }
		isTimeoutArmed = false
		timer?.cancel()
		timer = nil
		return true
	}

	private func fireTimeout(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		guard requested == generation, isTimeoutArmed else { return }
		// Снимаем onTermination: его Task с cancel(.normalClosure) гонялся бы
		// с нашим teardown и мог перебить close-код .goingAway
		outputContinuation.onTermination = nil
		outputContinuation.finish(throwing: NetworkStreamingError.timeout)
		delegateContinuation.finish(throwing: NetworkStreamingError.timeout)
		cancel(generation: requested, closeCode: .goingAway)
	}

	private func createInputTask(
		generation: Int,
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		guard generation == self.generation else { return }
		// detached: тело не трогает актора, а Task {} с наследованием изоляции
		// на части компиляторов неявно захватывает self — незавершающийся
		// inputStream удерживал бы актора от deinit
		inputTask = Task.detached {
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
					Logger.assistant.error(S("WebSocket input send failed"))
					// Причина фиксируется до отмены сокета (паттерн fireTimeout):
					// гонка didClose/didComplete не подменит семантику, потребитель
					// получит транспортную ошибку, а не «сервер договорил»
					delegateContinuation.finish(
						throwing: NetworkStreamingError.uplinkFailure(error as NSError)
					)
					task.cancel(with: .goingAway, reason: nil)
				}
			}
			guard Task.isCancelled == false, sendFailed == false else { return }
			Logger.assistant.info(S("Input stream finished"))
			do {
				try await task.send(.data(Self.inputTerminator))
			} catch {
				// Терминальное событие уже едет к потребителю через делегата —
				// здесь нужен лишь след в логе, что конец ввода не доехал
				Logger.assistant.error(S("WebSocket input terminator send failed"))
			}
		}
	}

	/// Заводит читателя и отдаёт сигнал его завершения; nil — поколение сдвинулось.
	private func createOutputTask(
		generation: Int,
		task: URLSessionWebSocketTask,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> AsyncStream<Void>? {
		// Хоп отменённой connectTask всё равно исполняется: без guard'а поздний
		// вызов пересоздал бы задачи после teardown или затёр слоты нового
		// поколения, оставив его живой inputTask без ссылки
		guard generation == self.generation else { return nil }
		let (done, doneContinuation) = AsyncStream<Void>.makeStream()
		let reader = Task<Void, Never> { [weak self] in
			// Сигнал завершения — от самого читателя: ожидание в drainOutput
			// становится отменяемым, промежуточный waiter не нужен
			defer { doneContinuation.finish() }
			var isFirstMessage = true
			var warnedTextFrame = false
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
						payload = text.data(using: .utf8)
					case .data(let data):
						payload = data
					@unknown default:
						payload = nil
					}
					// Таймер гасит первый кадр, ДОСТАВЛЕННЫЙ потребителю:
					// текстовый конвертируется и доставляется (с error-логом
					// о нарушении binary-контракта) — значит, тоже гасит;
					// @unknown-кадр отдать нечем — не гасит
					guard let payload else { continue }
					if isFirstMessage {
						isFirstMessage = false
						// Арбитраж «первый байт против таймаута» — одна точка
						// решения на акторе, победитель ровно один: либо таймаут
						// (сообщение считается опоздавшим и не доставляется),
						// либо первый байт (таймаут уже не выстрелит и все
						// последующие yield безопасны). Смешанного исхода
						// «данные, затем .timeout» не существует
						guard await self?.acceptFirstMessage(generation: generation) == true else {
							return
						}
					}
					if case .dropped = outputContinuation.yield(.received(payload)) {
						// Очередь переполнена: потребитель не вычитывает стрим
						// либо сервер заливает быстрее, чем тот успевает. Молча
						// терять кадры нельзя — стрим завершается ошибкой
						Logger.assistant.error(S("Output buffer overflow: consumer is not draining"))
						outputContinuation.finish(
							throwing: NetworkStreamingError.nsError(
								NSError(domain: NSURLErrorDomain, code: NSURLErrorDataLengthExceedsMaximum)
							)
						)
						return
					}
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
		generation: Int,
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
			// На отменённых путях дренаж намеренно вырождается в no-op: тот же
			// teardown отменил читателя строкой раньше, ждать уже нечего
			func drainOutput() async {
				guard let readerDone, Task.isCancelled == false else { return }
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
							generation: generation,
							outputContinuation: outputContinuation
						) == true else {
							continue
						}
						guard let created = await self?.createOutputTask(
							generation: generation,
							task: task,
							outputContinuation: outputContinuation
						) ?? nil else {
							// Отказ означает teardown в окне хопа: ранний return
							// подвесил бы continuation, которую может дочитывать
							// потребитель, — ждём штатного финиша через delegateStream
							continue
						}
						readerDone = created
						await self?.createInputTask(
							generation: generation,
							task: task,
							inputStream: inputStream,
							delegateContinuation: delegateContinuation
						)
					}
				}
			} catch {
				terminalError = error
			}
			await drainOutput()
			// Исход разрушенного стрима решает его инициатор, а не гонка с
			// терминальным событием того же teardown'а: вытеснение —
			// CancellationError, явная отмена — тихий finish. Терминальная
			// ошибка остаётся исходом там, где стрим умер сам
			// Снимаем onTermination перед финишем: иначе он планирует Task с
			// cancel() того же поколения, который следующей строкой отработает
			// и сам — хоп гарантированно холостой. Окна тут нет, между снятием
			// и финишем нет точки приостановки
			outputContinuation.onTermination = nil
			let outcome = Task.isCancelled ? await self?.outcome(for: lifecycleToken) : nil
			if outcome?.superseded == true {
				outputContinuation.finish(throwing: CancellationError())
			} else if outcome?.cancelled == true {
				outputContinuation.finish()
			} else if let terminalError {
				outputContinuation.finish(throwing: terminalError)
			} else {
				outputContinuation.finish()
			}
			if Task.isCancelled == false {
				await self?.cancel(generation: generation)
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
			.filter { Self.reservedCookieNames.contains($0.name) == false }
			.filter { Self.isSerializableCookie(name: $0.name, value: $0.value) }
			.sorted { Self.specificity(of: $0) < Self.specificity(of: $1) }
		for cookie in storedCookies {
			merged[cookie.name] = cookie
		}

		for cookie in Cookies.allCases {
			guard let value = await cookieStorage.getCookie(name: cookie.rawValue) else {
				// Рукопожатие уйдёт без авторизации, и сервер закроет его сам —
				// без этого лога причина отказа неотличима от серверной
				Logger.assistant.error(S("Missing SDK cookie: \(cookie.rawValue)"))
				continue
			}
			if let outCookie = createCookie(name: cookie.rawValue, value: value, for: url) {
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
			Logger.assistant.error(S("Cookie rejected for \(name): control characters or separators"))
			return nil
		}
		return HTTPCookie(properties: [
			.path: "/",
			.name: name,
			.value: value,
			.domain: url.host ?? ""
		])
	}

	/// Пара уходит в заголовок дословно, поэтому отсекаем всё, чем её можно
	/// разорвать: CTL (C0 и DEL) плюс разделители самого Cookie-заголовка.
	/// Категория Cf (U+FEFF, U+200B, мягкий перенос) намеренно разрешена —
	/// заголовок ей не разорвать, а CharacterSet.controlCharacters забраковал
	/// бы по ней валидный токен
	private static func isSerializableCookie(name: String, value: String) -> Bool {
		name.isEmpty == false && isValidCookieName(name) && isValidCookieValue(value)
	}

	/// '=' в имени разорвало бы пару, поэтому запрещён именно здесь
	private static func isValidCookieName(_ name: String) -> Bool {
		isCookieOctets(name) && name.contains("=") == false
	}

	/// '=' внутри значения легален (RFC 6265 4.1.1) и обязателен для base64 с
	/// паддингом: сервер режет пару по ПЕРВОМУ '=', так что подменить ничего нельзя
	private static func isValidCookieValue(_ value: String) -> Bool {
		isCookieOctets(value)
	}

	private static func isCookieOctets(_ text: String) -> Bool {
		text.unicodeScalars.allSatisfy { scalar in
			scalar.value >= 0x20 && scalar.value != 0x7F
				&& scalar != ";" && scalar != ","
		}
	}

	// MARK: - Header Validation

	/// RFC 7230 token: имя с управляющими символами Foundation принимает
	/// дословно, поэтому отбраковываем его сами. Пробел и таб уже отсечены
	/// проверкой `> 0x20`, в наборе их нет
	private static let headerNameSeparators = Set("()<>@,;:\\\"/[]?={}")

	private static func isValidHeaderName(_ name: String) -> Bool {
		guard name.isEmpty == false else { return false }
		return name.unicodeScalars.allSatisfy { scalar in
			scalar.isASCII && scalar.value > 0x20 && scalar.value != 0x7F
				&& headerNameSeparators.contains(Character(scalar)) == false
		}
	}

	/// Значение с CR/LF/NUL Foundation отбрасывает вместе со всем заголовком —
	/// молча. HTAB и пробел легальны
	private static func isValidHeaderValue(_ value: String) -> Bool {
		value.unicodeScalars.allSatisfy { scalar in
			scalar.value == 0x09 || (scalar.value >= 0x20 && scalar.value != 0x7F)
		}
	}

	/// Управляющие символы схлопываются в '?': строчный лог-сток иначе принял бы
	/// перевод строки из чужого имени за начало новой записи
	private static func logSafe(_ text: String) -> String {
		let scrubbed = String(String.UnicodeScalarView(text.unicodeScalars.map { scalar in
			scalar.value < 0x20 || scalar.value == 0x7F ? "?" : scalar
		}))
		return scrubbed.count > 64 ? String(scrubbed.prefix(64)) + "…" : scrubbed
	}
}

// MARK: - NetworkStreamingError

extension NetworkStreamingError {

	/// Маппинг сбоя аплинка. Кейс потери соединения общий с делегатом: иначе
	/// один и тот же обрыв давал бы .timeout или .nsError в зависимости от того,
	/// кто заметил его первым — читатель или inflight-send. Ветка делегата
	/// «POSIX 57 → чистый финиш» сюда не переносится: сбой отправки не должен
	/// выглядеть как «сервер договорил»
	static func uplinkFailure(_ error: NSError) -> NetworkStreamingError {
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorNetworkConnectionLost {
			return .timeout
		}
		return .nsError(error)
	}
}

// MARK: - WebSocketNetworkStreamingDelegate

final class WebSocketNetworkStreamingDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {

	private let origin: URL
	private let continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation

	init(origin: URL, continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation) {
		self.origin = origin
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
		guard Self.isCleanClosure(closeCode) else {
			continuation.finish(throwing: NetworkStreamingError.closeCode(NetworkStreamingOutputError(code: closeCode)))
			return
		}
		continuation.finish()
	}

	/// 1005 — «кода не было», легальный исход по RFC 6455 §7.1.5, а не сбой
	static func isCleanClosure(_ closeCode: URLSessionWebSocketTask.CloseCode) -> Bool {
		closeCode == .normalClosure || closeCode == .noStatusReceived
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
		completionHandler: @escaping (URLRequest?) -> Void
	) {
		guard let target = request.url, isSameOrigin(target) else {
			Logger.assistant.error(S("WebSocket handshake redirect refused"))
			// Причина фиксируется ДО возврата управления (паттерн fireTimeout):
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
		guard let error = error as NSError? else {
			// Терминальное событие без ошибки: если CFNetwork потерял didCloseWith,
			// abnormal-код восстанавливаем из задачи (симметрично ветке ENOTCONN)
			if let closeError = recoveredCloseError(from: task) {
				continuation.finish(throwing: closeError)
				return
			}
			continuation.finish()
			return
		}
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorCancelled {
			// Соединение закрыли мы сами (в т.ч. отмена задач в deinit) —
			// для потребителя это не ошибка
			continuation.finish()
			return
		}
		if error.domain == NSPOSIXErrorDomain && error.code == Int(POSIXErrorCode.ENOTCONN.rawValue) {
			// POSIX 57: сокет закрыт к моменту completion — финал гонки с
			// close-фреймом. didCloseWith теряется безотносительно кода
			// (замерено), код восстанавливаем из задачи. Истинные обрывы
			// приходят NSURLError-кодами
			if let closeError = recoveredCloseError(from: task) {
				continuation.finish(throwing: closeError)
				return
			}
			continuation.finish()
			return
		}
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorNetworkConnectionLost {
			// Унаследованный контракт SDK: потребители различают исходы по этим
			// кейсам, менять только синхронно с вызывающим кодом
			continuation.finish(throwing: NetworkStreamingError.timeout)
			return
		}
		continuation.finish(throwing: NetworkStreamingError.nsError(error))
	}

	private func isSameOrigin(_ target: URL) -> Bool {
		let source = WebSocketNetworkStreaming.cookieMatchURL(for: origin)
		let destination = WebSocketNetworkStreaming.cookieMatchURL(for: target)
		guard let sourceScheme = source.scheme?.lowercased(),
			  let destinationScheme = destination.scheme?.lowercased(),
			  let sourceHost = source.host?.lowercased(),
			  let destinationHost = destination.host?.lowercased() else {
			return false
		}
		return sourceScheme == destinationScheme
			&& sourceHost == destinationHost
			&& Self.port(of: source) == Self.port(of: destination)
	}

	private static func port(of url: URL) -> Int? {
		if let port = url.port { return port }
		switch url.scheme?.lowercased() {
		case "https": return 443
		case "http": return 80
		default: return nil
		}
	}

	private func recoveredCloseError(from task: URLSessionTask) -> NetworkStreamingError? {
		guard let webSocketTask = task as? URLSessionWebSocketTask else { return nil }
		let closeCode = webSocketTask.closeCode
		guard closeCode != .invalid, Self.isCleanClosure(closeCode) == false else { return nil }
		return NetworkStreamingError.closeCode(NetworkStreamingOutputError(code: closeCode))
	}
}
