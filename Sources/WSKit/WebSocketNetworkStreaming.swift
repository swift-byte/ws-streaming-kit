import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

actor WebSocketNetworkStreaming: NetworkStreaming {

	// MARK: - Private Types

	/// Токен жизненного цикла стрима: вытеснитель помечает токен жертвы, и
	/// жертва решает исход по СВОЕМУ токену. Флаг пишется и читается только
	/// в изоляции актора; @unchecked — лишь ради захвата замыканием connectTask
	private final class LifecycleToken: @unchecked Sendable {
		var superseded = false
	}

	private enum TimeoutArbitration {
		case pending
		case firstMessage
		case timedOut
		case cancelled
	}

	// MARK: - Private Properties

	private static let inputTerminator = Data([0x31])
	// Запас на медленные среды: 0.5с force-закрывал сокет посреди доставки
	// хвоста (замерено на iOS-симуляторе, 4/5 сообщений). В happy path
	// дедлайн снимается сразу; полные 3с платятся лишь в патологии
	// незавершившегося читателя — терминальное событие тогда ждёт грейс.
	// Это граница ожидания дренажа: по её истечении стрим финиширует
	// без читателя
	private static let drainGraceNanoseconds: UInt64 = 3_000_000_000

	private let kidsURLSession: KidsURLSession
	private let cookieStorage: CookieStorage
	private let timeout: Int
	private let session: URLSession
	private let handshakeCookieStorage: HTTPCookieStorage?

	private var generation = 0
	private var currentToken: LifecycleToken?
	private var timeoutArbitration = TimeoutArbitration.pending
	private var webSocketTask: URLSessionWebSocketTask?
	private var outputTask: Task<Void, Never>?
	private var inputTask: Task<Void, Never>?
	private var connectTask: Task<Void, Never>?
	private var timer: Task<Void, Never>?

	// MARK: - Init

	/// - Parameter timeout: секунды ожидания первого входящего сообщения;
	///   значения вне 1...3600 клампятся к границам.
	init(
		kidsURLSession: KidsURLSession,
		cookieStorage: CookieStorage,
		timeout: Int? = nil
	) {
		self.kidsURLSession = kidsURLSession
		self.cookieStorage = cookieStorage
		// Нижняя граница спасает UInt64-конверсию от отрицательного значения
		// (иначе трап); верхняя — санитарный предел, до переполнения ей далеко
		self.timeout = min(max(1, timeout ?? 15), 3_600)

		let configuration = URLSessionConfiguration.default
		configuration.httpCookieStorage = .shared
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
		webSocketTask?.cancel(with: .normalClosure, reason: nil)
		inputTask?.cancel()
		outputTask?.cancel()
		connectTask?.cancel()
		timer?.cancel()
		// invalidateAndCancel рвал соединение раньше, чем улетал close-фрейм
		// строкой выше — graceful-закрытие превращалось в abnormal
		session.finishTasksAndInvalidate()
	}

	// MARK: - NetworkStreaming

	/// Устанавливает соединение и возвращает стрим событий.
	/// - Каждому вызову нужен свежий inputStream: AsyncStream одноразов и
	///   single-consumer, повторная передача делит данные недетерминированно.
	/// - До .connected inputStream не потребляется; при отказе на этой фазе он
	///   освобождается, продюсеру приходит onTermination.
	/// - badURL — единственный отказ без teardown: действующий стрим сохраняется.
	/// - Endpoint доверен целиком, включая схему: это SDK-внутренняя константа,
	///   валидация даунгрейда wss→ws — забота конфигурации и ревью, не рантайма.
	///   Пересмотреть, если endpoint станет конфигурируемым.
	func establishStream(
		endpoint: String,
		headers: [String: String],
		inputStream: AsyncStream<Data>
	) async throws -> AsyncThrowingStream<AssistantSDK.NetworkStreamingOutputEvent, any Error> {
		guard let url = URL(string: endpoint) else {
			throw URLError(.badURL)
		}

		// Вытеснение: помечается токен текущего владельца — живого стрима или
		// вызова в окне кук. Глобальной бухгалтерии нет: токен умирает вместе
		// со своим стримом, протухать нечему, затирать нечего
		currentToken?.superseded = true
		await cancel()
		let currentGeneration = generation
		let lifecycleToken = LifecycleToken()
		currentToken = lifecycleToken

		// Буфер выходного стрима unbounded: потребитель голосового стрима
		// обязан поспевать, backpressure не применяется намеренно
		let (outputStream, outputContinuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.makeStream()
		let (delegateStream, delegateContinuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.makeStream()

		outputContinuation.onTermination = { [weak self] _ in
			Task { await self?.cancel(generation: currentGeneration) }
		}

		var request = URLRequest(url: url)
		for (key, value) in headers {
			request.setValue(value, forHTTPHeaderField: key)
		}

		await addCookies(to: &request, for: url)

		guard generation == currentGeneration else {
			// Пока собирали куки, поколение сдвинулось. Причину различаем по
			// токену: вытеснение — CancellationError (ретрай-логика не должна
			// путать замену со штатным закрытием), явная отмена — тихий finish,
			// единый с контрактом поднятого стрима
			if lifecycleToken.superseded {
				outputContinuation.finish(throwing: CancellationError())
			} else {
				outputContinuation.finish()
			}
			delegateContinuation.finish()
			return outputStream
		}

		timeoutArbitration = .pending
		let delegate = WebSocketNetworkStreamingDelegate(continuation: delegateContinuation)
		let task = session.webSocketTask(with: request)
		webSocketTask = task
		task.delegate = delegate
		task.resume()

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

		createCheckConnectTask(
			generation: currentGeneration,
			lifecycleToken: lifecycleToken,
			task: task,
			inputStream: inputStream,
			with: delegateStream,
			outputContinuation: outputContinuation
		)

		return outputStream
	}

	/// Закрывает вебсокет-соединение и отменяет его задачи. Уже буферизованный
	/// платформой хвост входящих может быть доставлен до финиша.
	func cancel() async {
		cancel(generation: generation)
	}

	/// Останавливает таймер таймаута текущего поколения. Снимает единственную
	/// страховку от зависшего рукопожатия: дальше дедлайнами управляет вызывающий.
	func invalidate() {
		timer?.cancel()
		timer = nil
	}

	// MARK: - Private Methods

	// Тело обязано оставаться синхронным (без await): establishStream
	// полагается на неразрывность «пометка токена → teardown → чтение
	// поколения» внутри своей изоляции.
	// closeCode — замеренная доставка до сервера: iOS отдаёт 1001 (и голый
	// путь, и наш), macOS — всегда 1006 (дефект платформы: и без отмены
	// читателя, и с висящим receive), Linux — недетерминирован (0/1001).
	// На целевой платформе код работает — параметр не мёртвый вес
	private func cancel(
		generation requested: Int,
		closeCode: URLSessionWebSocketTask.CloseCode = .normalClosure
	) {
		guard requested == generation else { return }
		// Терминальное состояние арбитража: после отмены ни поздний таймер,
		// ни поздний первый байт «выиграть» не могут — иначе на границе
		// истечения потребитель, сам вызвавший cancel(), мог получить ложный
		// .timeout. Уже принятые исходы не затираем
		if timeoutArbitration == .pending {
			timeoutArbitration = .cancelled
		}
		webSocketTask?.cancel(with: closeCode, reason: nil)
		webSocketTask = nil
		inputTask?.cancel()
		inputTask = nil
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

	private func isSuperseded(_ token: LifecycleToken) -> Bool {
		token.superseded
	}

	private func acceptFirstMessage(generation requested: Int) -> Bool {
		// Вызывается один раз на поколение по построению: isFirstMessage
		// локален единственному читателю — повторного арбитража не существует
		guard requested == generation, timeoutArbitration == .pending else { return false }
		timeoutArbitration = .firstMessage
		timer?.cancel()
		timer = nil
		return true
	}

	private func fireTimeout(
		generation requested: Int,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation,
		delegateContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		guard requested == generation, timeoutArbitration == .pending else { return }
		timeoutArbitration = .timedOut
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
		inputStream: AsyncStream<Data>
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
					sendFailed = true
					Logger.assistant.error(S("WebSocket input send failed; closing connection"))
					// Молчаливый полуживой стрим хуже разрыва: закрываем сокет,
					// потребитель получит терминальное событие через didComplete
					task.cancel(with: .goingAway, reason: nil)
				}
			}
			guard Task.isCancelled == false, sendFailed == false else { return }
			Logger.assistant.info(S("Input stream finished"))
			try? await task.send(.data(Self.inputTerminator))
		}
	}

	private func createOutputTask(
		generation: Int,
		task: URLSessionWebSocketTask,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> (reader: Task<Void, Never>, done: AsyncStream<Void>)? {
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
					// Арбитраж — по доставляемому байту: кадр, который нечем
					// отдать потребителю, таймер не гасит
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
					outputContinuation.yield(.received(payload))
				}
			} catch {
				// Терминальная ошибка чтения: завершение стрима — за connectTask,
				// который сперва дожидается доставки хвоста сообщений здесь
			}
		}
		outputTask = reader
		return (reader, done)
	}

	private func createCheckConnectTask(
		generation: Int,
		lifecycleToken: LifecycleToken,
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>,
		with delegateStream: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		connectTask = Task { [weak self] in
			var reader: Task<Void, Never>?
			var readerDone: AsyncStream<Void>?

			// Финиш стрима — только после доставки хвоста читателем. Ожидание
			// ограничено гонкой «сигнал завершения читателя против дедлайна»:
			// обе ветви группы отменяемы, группа не зависает. Патология
			// (receive() не проснулся на закрытом сокете) оставляет висеть
			// только сам читатель — минимально возможная цена
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
					if case .connected = event, reader == nil {
						// .connected выдаётся изолированно, одним шагом с проверкой
						// поколения: спуриозного connected для мёртвого поколения
						// не существует; teardown после выдачи — легитимная
						// последовательность connected→cancelled. Reader создаётся
						// после yield — иначе .received мог бы обогнать .connected
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
						reader = created.reader
						readerDone = created.done
						await self?.createInputTask(
							generation: generation,
							task: task,
							inputStream: inputStream
						)
					}
				}
				await drainOutput()
				if Task.isCancelled, await self?.isSuperseded(lifecycleToken) == true {
					// Контракт вытеснения един для всех фаз жизни стрима:
					// замена — это CancellationError, а не «сервер закрылся»
					outputContinuation.finish(throwing: CancellationError())
				} else {
					outputContinuation.finish()
				}
			} catch {
				await drainOutput()
				outputContinuation.finish(throwing: error)
			}
			if Task.isCancelled == false {
				await self?.cancel(generation: generation)
			}
		}
	}

	// MARK: - Cookies

	func addCookies(to request: inout URLRequest, for url: URL) async {
		// Куки прикрепляются к переданному endpoint как есть: он SDK-внутренний
		// и доверенный. Cookie-заголовок вызывающего снимается безусловно —
		// заголовком владеет только этот пайплайн.
		// Авто-обработку отключаем: иначе URLSession может перетереть собранный
		// вручную Cookie значениями из shared-стоража. ВАЖНО: это отключает и
		// сохранение Set-Cookie из ответа на handshake — если бэк ротирует куки
		// на рукопожатии, их нужно подхватывать отдельно
		request.httpShouldHandleCookies = false
		request.setValue(nil, forHTTPHeaderField: "Cookie")

		// Слияние вместо замещения: свои куки перекрывают одноимённые из
		// shared-стоража, остальные сохраняются. Ключ — имя, намеренно без
		// path: path-scoped дубликат положил бы протухшее значение рядом со
		// свежим. Схема нормализуется (ws→http, wss→https), чтобы матчинг
		// стоража и secure-куки работали
		var merged = [String: HTTPCookie]()

		// Зарезервированные имена не берутся из shared вовсе: чужая кука
		// хост-приложения не должна молча подменять авторизацию SDK.
		// Одноимённые из стоража упорядочены по специфичности path —
		// длинный path перекрывает короткий; порядок тотальный (при равной
		// длине — лексикографический), sort в Swift нестабилен
		let reservedNames = Set(Cookies.allCases.map(\.rawValue))
		let storedCookies = (handshakeCookieStorage?.cookies(for: cookieMatchURL(for: url)) ?? [])
			.filter { reservedNames.contains($0.name) == false }
			.sorted { ($0.path.count, $0.path) < ($1.path.count, $1.path) }
		for cookie in storedCookies {
			merged[cookie.name] = cookie
		}

		for cookie in Cookies.allCases {
			if let value = await cookieStorage.getCookie(name: cookie.rawValue),
			   let outCookie = createCookie(name: cookie.rawValue, value: value, for: url) {
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

	private func cookieMatchURL(for url: URL) -> URL {
		guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else { return url }
		switch components.scheme?.lowercased() {
		case "ws": components.scheme = "http"
		case "wss": components.scheme = "https"
		default: break
		}
		return components.url ?? url
	}

	func createCookie(name: String, value: String, for url: URL) -> HTTPCookie? {
		guard value.rangeOfCharacter(from: .newlines) == nil, value.contains(";") == false else {
			Logger.assistant.error(S("Cookie value rejected: control characters or separators"))
			return nil
		}
		return HTTPCookie(properties: [
			.path: "/",
			.name: name,
			.value: value,
			.domain: url.host ?? ""
		])
	}
}

// MARK: - WebSocketNetworkStreamingDelegate

final class WebSocketNetworkStreamingDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {

	private let continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation

	init(continuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation) {
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
		guard closeCode != .normalClosure else {
			continuation.finish()
			return
		}
		continuation.finish(throwing: NetworkStreamingError.closeCode(NetworkStreamingOutputError(code: closeCode)))
	}
}

// MARK: - URLSessionTaskDelegate

extension WebSocketNetworkStreamingDelegate: URLSessionTaskDelegate {

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
			// Сокет уже закрыт к моменту completion (POSIX 57) — финал гонки
			// с close-фреймом (ветка из исходной версии файла). CFNetwork может
			// потерять колбэк didCloseWith безотносительно кода закрытия
			// (замерено для 1000 на macOS CI), поэтому код восстанавливаем из
			// самой задачи — abnormal close не схлопывается в чистый финиш.
			// Истинные обрывы приходят NSURLError-кодами (-1005/-1001)
			if let closeError = recoveredCloseError(from: task) {
				continuation.finish(throwing: closeError)
				return
			}
			continuation.finish()
			return
		}
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorNetworkConnectionLost {
			// Маппинг из референсной реализации — унаследованный контракт SDK:
			// потребители различают исходы по этим кейсам, менять только
			// синхронно с вызывающим кодом
			continuation.finish(throwing: NetworkStreamingError.timeout)
			return
		}
		continuation.finish(throwing: NetworkStreamingError.nsError(error))
	}

	private func recoveredCloseError(from task: URLSessionTask) -> NetworkStreamingError? {
		guard let webSocketTask = task as? URLSessionWebSocketTask else { return nil }
		let closeCode = webSocketTask.closeCode
		guard closeCode != .invalid && closeCode != .normalClosure else { return nil }
		return NetworkStreamingError.closeCode(NetworkStreamingOutputError(code: closeCode))
	}
}
