import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

actor WebSocketNetworkStreaming: NetworkStreaming {

	// MARK: - Private Properties

	private static let inputTerminator = Data([0x31])
	private static let drainGraceNanoseconds: UInt64 = 500_000_000

	private let kidsURLSession: KidsURLSession
	private let cookieStorage: CookieStorage
	private let timeout: Int
	private let session: URLSession

	private var generation = 0
	private var webSocketTask: URLSessionWebSocketTask?
	private var outputTask: Task<Void, Never>?
	private var inputTask: Task<Void, Never>?
	private var connectTask: Task<Void, Never>?
	private var timer: Task<Void, Never>?

	// MARK: - Init

	init(
		kidsURLSession: KidsURLSession,
		cookieStorage: CookieStorage,
		timeout: Int? = nil
	) {
		self.kidsURLSession = kidsURLSession
		self.cookieStorage = cookieStorage
		self.timeout = timeout ?? 15

		let configuration = URLSessionConfiguration.default
		configuration.httpCookieStorage = .shared
		// Авто-обработка кук на запросах отключена (см. addCookies),
		// поэтому acceptPolicy на эту сессию фактически не влияет
		configuration.httpCookieAcceptPolicy = .always
		session = URLSession(
			configuration: configuration,
			delegate: kidsURLSession,
			delegateQueue: nil
		)
	}

	// MARK: - Deinit

	deinit {
		webSocketTask?.cancel(with: .normalClosure, reason: nil)
		inputTask?.cancel()
		outputTask?.cancel()
		connectTask?.cancel()
		timer?.cancel()
		session.invalidateAndCancel()
	}

	// MARK: - NetworkStreaming

	func establishStream(
		endpoint: String,
		headers: [String: String],
		inputStream: AsyncStream<Data>
	) async throws -> AsyncThrowingStream<AssistantSDK.NetworkStreamingOutputEvent, any Error> {
		guard let url = URL(string: endpoint) else {
			throw URLError(.badURL)
		}

		await cancel()
		generation &+= 1
		let currentGeneration = generation

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
			// Пока собирали куки, стрим переустановили: это поколение устарело.
			// CancellationError, а не тихий finish — чтобы ретрай-логика выше
			// не приняла замену за штатное закрытие сервером
			outputContinuation.finish(throwing: CancellationError())
			delegateContinuation.finish()
			return outputStream
		}

		let delegate = WebSocketNetworkStreamingDelegate(continuation: delegateContinuation)
		let task = session.webSocketTask(with: request)
		webSocketTask = task
		task.delegate = delegate
		task.resume()

		let timeoutSeconds = UInt64(max(0, timeout))
		timer = Task { [weak self] in
			try? await Task.sleep(nanoseconds: timeoutSeconds * 1_000_000_000)
			if Task.isCancelled { return }
			// Снимаем onTermination: его Task с cancel(.normalClosure) гонялся бы
			// с нашим teardown и мог перебить close-код .goingAway
			outputContinuation.onTermination = nil
			outputContinuation.finish(throwing: NetworkStreamingError.timeout)
			delegateContinuation.finish(throwing: NetworkStreamingError.timeout)
			await self?.cancel(generation: currentGeneration, closeCode: .goingAway)
		}

		createCheckConnectTask(
			generation: currentGeneration,
			task: task,
			inputStream: inputStream,
			with: delegateStream,
			outputContinuation: outputContinuation
		)

		return outputStream
	}

	/// Закрывает вебсокет-соединение и отменяет его задачи.
	func cancel() async {
		cancel(generation: generation)
	}

	/// Останавливает таймер таймаута.
	func invalidate() {
		timer?.cancel()
		timer = nil
	}

	// MARK: - Private Methods

	private func cancel(
		generation requested: Int,
		closeCode: URLSessionWebSocketTask.CloseCode = .normalClosure
	) {
		guard requested == generation else { return }
		webSocketTask?.cancel(with: closeCode, reason: nil)
		webSocketTask = nil
		inputTask?.cancel()
		inputTask = nil
		// Читателя swift-отменой не трогаем: cancellation-handler асинхронного
		// receive() на Darwin делает плоский task.cancel() и срывает
		// graceful-close (сервер видит 1006 вместо кода). Читатель выйдет сам:
		// закрытый сокет роняет receive()
		outputTask = nil
		connectTask?.cancel()
		connectTask = nil
		timer?.cancel()
		timer = nil
	}

	private func invalidate(generation requested: Int) {
		guard requested == generation else { return }
		timer?.cancel()
		timer = nil
	}

	private func createInputTask(
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>
	) {
		// detached: тело не трогает актора, а Task {} с наследованием изоляции
		// на части компиляторов неявно захватывает self — незавершающийся
		// inputStream удерживал бы актора от deinit
		inputTask = Task.detached {
			for await data in inputStream {
				try? await task.send(.data(data))
			}
			guard Task.isCancelled == false else { return }
			Logger.assistant.info(S("Input stream finished"))
			try? await task.send(.data(Self.inputTerminator))
		}
	}

	@discardableResult
	private func createOutputTask(
		generation: Int,
		task: URLSessionWebSocketTask,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) -> Task<Void, Never> {
		let reader = Task<Void, Never> { [weak self] in
			var didInvalidate = false
			do {
				while Task.isCancelled == false {
					let message = try await task.receive()
					if didInvalidate == false {
						didInvalidate = true
						await self?.invalidate(generation: generation)
					}
					switch message {
					case .string(let text):
						Logger.assistant.error(S("We expect binary data here"))
						if let data = text.data(using: .utf8) {
							outputContinuation.yield(.received(data))
						}
					case .data(let data):
						outputContinuation.yield(.received(data))
					@unknown default:
						continue
					}
				}
			} catch {
				// Терминальная ошибка чтения: завершение стрима — за connectTask,
				// который сперва дожидается доставки хвоста сообщений здесь
			}
		}
		outputTask = reader
		return reader
	}

	private func createCheckConnectTask(
		generation: Int,
		task: URLSessionWebSocketTask,
		inputStream: AsyncStream<Data>,
		with delegateStream: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>,
		outputContinuation: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.Continuation
	) {
		connectTask = Task { [weak self] in
			var reader: Task<Void, Never>?

			// Финиш стрима — только после того, как читатель доставил хвост
			// сообщений: иначе close-фрейм обгоняет данные и последние сообщения
			// теряются. Если платформа не разбудила receive() после закрытия,
			// дожимаем сокет по дедлайну. Это толчок, а не жёсткая граница:
			// receive(), зависший на уже закрытом сокете, дедлайн не разбудит
			func drainOutput() async {
				guard let reader else { return }
				let deadline = Task { [weak self] in
					try? await Task.sleep(nanoseconds: Self.drainGraceNanoseconds)
					if Task.isCancelled { return }
					await self?.cancel(generation: generation)
				}
				await reader.value
				deadline.cancel()
			}

			do {
				for try await event in delegateStream {
					if case .connected = event, reader == nil {
						outputContinuation.yield(.connected)
						reader = await self?.createOutputTask(
							generation: generation,
							task: task,
							outputContinuation: outputContinuation
						)
						await self?.createInputTask(
							task: task,
							inputStream: inputStream
						)
					}
				}
				await drainOutput()
				outputContinuation.finish()
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
		// Авто-обработку отключаем: иначе URLSession может перетереть собранный
		// вручную Cookie значениями из shared-стоража. ВАЖНО: это отключает и
		// сохранение Set-Cookie из ответа на handshake — если бэк ротирует куки
		// на рукопожатии, их нужно подхватывать отдельно
		request.httpShouldHandleCookies = false

		// Слияние вместо замещения: свои куки перекрывают одноимённые из
		// shared-стоража, остальные из стоража сохраняются. Схема нормализуется
		// (ws→http, wss→https), чтобы матчинг стоража и secure-куки работали
		var merged = [String: HTTPCookie]()

		for cookie in session.configuration.httpCookieStorage?.cookies(for: cookieMatchURL(for: url)) ?? [] {
			merged[cookie.name] = cookie
		}

		for cookie in Cookies.allCases {
			if let value = await cookieStorage.getCookie(name: cookie.rawValue),
			   let outCookie = createCookie(name: cookie.rawValue, value: value, for: url) {
				merged[outCookie.name] = outCookie
			}
		}

		guard merged.isEmpty == false else { return }

		let cookieHeaders = HTTPCookie.requestHeaderFields(with: Array(merged.values))

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
		HTTPCookie(properties: [
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
			// Терминальное событие задачи: даже без ошибки и close-фрейма
			// стрим должен завершиться, а не ждать внешней отмены
			continuation.finish()
			return
		}
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorCancelled {
			// Соединение закрыли мы сами (в т.ч. invalidateAndCancel в deinit) —
			// для потребителя это не ошибка
			continuation.finish()
			return
		}
		if error.domain == NSURLErrorDomain && error.code == NSURLErrorNetworkConnectionLost {
			continuation.finish(throwing: NetworkStreamingError.timeout)
			return
		}
		continuation.finish(throwing: NetworkStreamingError.nsError(error))
	}
}
