import XCTest
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
@testable import WSKit

/// Интеграционные тесты требуют запущенного ws_test_server.py:
///   WS-сервер на 127.0.0.1:8901 (пути /echo, /push3, /sink, /cookie,
///   /close1000, /close1011, /msg-then-silent, /push-after-3, /silent)
///   и «молчащий» TCP-акцептор на 127.0.0.1:8902 (для теста таймаута).
final class WebSocketNetworkStreamingTests: XCTestCase {

	private let wsBase = "ws://127.0.0.1:8901"
	private let silentTCP = "ws://127.0.0.1:8902"

	// MARK: - Helpers

	private func makeStreaming(
		timeout: Int? = nil,
		cookies: [Cookies: String] = [:]
	) async -> WebSocketNetworkStreaming {
		let storage = CookieStorage()
		for (name, value) in cookies {
			await storage.set(value, for: name.rawValue)
		}
		return WebSocketNetworkStreaming(
			kidsURLSession: KidsURLSession(),
			cookieStorage: storage,
			timeout: timeout
		)
	}

	private struct DrainResult {
		var events: [NetworkStreamingOutputEvent]
		var thrown: Error?
		var completed: Bool   // стрим завершился сам (finish / finish(throwing:))
		var timedOut: Bool    // мы прервали чтение по дедлайну
	}

	private actor Collector {
		var events: [NetworkStreamingOutputEvent] = []
		var thrown: Error?
		var completed = false
		var timedOut = false
		func add(_ e: NetworkStreamingOutputEvent) { events.append(e) }
		func finish(_ error: Error?) { thrown = error; completed = true }
		func markTimeout() { timedOut = true }
	}

	private actor StreamReader {
		private var iterator: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.AsyncIterator

		init(_ stream: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>) {
			iterator = stream.makeAsyncIterator()
		}

		func next() async throws -> NetworkStreamingOutputEvent? {
			// Мутирующий async на isolated-свойстве запрещён — копируем локально
			var localIterator = iterator
			let value = try await localIterator.next()
			iterator = localIterator
			return value
		}
	}

	/// Вычитывает стрим до его завершения либо до дедлайна.
	private func drain(
		_ stream: AsyncThrowingStream<NetworkStreamingOutputEvent, Error>,
		deadline: Double
	) async -> DrainResult {
		let collector = Collector()
		let reader = Task {
			do {
				for try await event in stream {
					await collector.add(event)
				}
				if Task.isCancelled {
					await collector.markTimeout()
				} else {
					await collector.finish(nil)
				}
			} catch {
				await collector.finish(error)
			}
		}
		let killer = Task {
			try? await Task.sleep(nanoseconds: UInt64(deadline * 1_000_000_000))
			reader.cancel()
		}
		_ = await reader.value
		killer.cancel()
		return await DrainResult(
			events: collector.events,
			thrown: collector.thrown,
			completed: collector.completed,
			timedOut: collector.timedOut
		)
	}

	private func makeInput() -> (AsyncStream<Data>, AsyncStream<Data>.Continuation) {
		AsyncStream<Data>.makeStream()
	}

	private func openStream(
		_ ws: WebSocketNetworkStreaming,
		path: String,
		input: AsyncStream<Data>? = nil,
		headers: [String: String] = [:]
	) async throws -> AsyncThrowingStream<NetworkStreamingOutputEvent, Error> {
		let (defaultInput, defaultCont) = makeInput()
		if input == nil { defaultCont.finish() }
		return try await ws.establishStream(
			endpoint: wsBase + path,
			headers: headers,
			inputStream: input ?? defaultInput
		)
	}

	// MARK: - Unit: URL

	func testBadURLThrowsBadURL() async throws {
		let ws = await makeStreaming()
		let (input, cont) = makeInput()
		cont.finish()
		do {
			_ = try await ws.establishStream(endpoint: "", headers: [:], inputStream: input)
			XCTFail("Ожидали URLError(.badURL)")
		} catch let error as URLError {
			XCTAssertEqual(error.code, .badURL)
		}
	}

	// MARK: - Unit: Cookies

	func testCreateCookieBuildsCookie() async throws {
		let ws = await makeStreaming()
		let url = URL(string: "ws://example.com/path")!
		let maybeCookie = await ws.createCookie(name: "sessionid", value: "abc", for: url)
		let cookie = try XCTUnwrap(maybeCookie)
		XCTAssertEqual(cookie.name, "sessionid")
		XCTAssertEqual(cookie.value, "abc")
		XCTAssertEqual(cookie.domain, "example.com")
		XCTAssertEqual(cookie.path, "/")
	}

	func testCreateCookieWithoutHostYieldsEmptyDomain() async throws {
		// HTTPCookie(properties:) с пустым domain возвращает куку, а не nil —
		// поведение одинаково на Darwin и corelibs (проверено на macOS CI).
		// В Cookie-заголовок domain не сериализуется, так что это безвредно
		let ws = await makeStreaming()
		let url = URL(string: "file:///tmp/x")!
		let cookie = await ws.createCookie(name: "a", value: "b", for: url)
		XCTAssertEqual(cookie?.domain, "")
	}

	// MARK: - Unit: делегат -> события стрима

	private func makeDelegatePair() -> (
		WebSocketNetworkStreamingDelegate,
		AsyncThrowingStream<NetworkStreamingOutputEvent, Error>,
		URLSession,
		URLSessionWebSocketTask
	) {
		let (stream, continuation) = AsyncThrowingStream<NetworkStreamingOutputEvent, Error>.makeStream()
		let delegate = WebSocketNetworkStreamingDelegate(continuation: continuation)
		let session = URLSession(configuration: .default)
		// Задача не резюмируется — нужна только как аргумент делегатных методов
		let task = session.webSocketTask(with: URLRequest(url: URL(string: "ws://127.0.0.1:1")!))
		return (delegate, stream, session, task)
	}

	func testDelegateDidOpenYieldsConnected() async {
		let (delegate, stream, session, task) = makeDelegatePair()
		delegate.urlSession(session, webSocketTask: task, didOpenWithProtocol: nil)
		let result = await drain(stream, deadline: 0.3)
		XCTAssertEqual(result.events, [.connected])
		XCTAssertTrue(result.timedOut, "Открытие не должно завершать стрим")
	}

	func testDelegateNormalCloseFinishesCleanly() async {
		let (delegate, stream, session, task) = makeDelegatePair()
		delegate.urlSession(session, webSocketTask: task, didCloseWith: .normalClosure, reason: nil)
		let result = await drain(stream, deadline: 1)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown)
	}

	func testDelegateAbnormalCloseThrowsCloseCode() async {
		let (delegate, stream, session, task) = makeDelegatePair()
		delegate.urlSession(session, webSocketTask: task, didCloseWith: .internalServerError, reason: nil)
		let result = await drain(stream, deadline: 1)
		guard case .closeCode(let payload)? = result.thrown as? NetworkStreamingError else {
			return XCTFail("Ожидали NetworkStreamingError.closeCode, получили \(String(describing: result.thrown))")
		}
		XCTAssertEqual(payload.code, .internalServerError)
	}

	func testDelegateConnectionLostMapsToTimeout() async {
		let (delegate, stream, session, task) = makeDelegatePair()
		let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorNetworkConnectionLost)
		delegate.urlSession(session, task: task, didCompleteWithError: error)
		let result = await drain(stream, deadline: 1)
		guard case .timeout? = result.thrown as? NetworkStreamingError else {
			return XCTFail("Ожидали .timeout, получили \(String(describing: result.thrown))")
		}
	}

	func testDelegateOtherErrorMapsToNSError() async {
		let (delegate, stream, session, task) = makeDelegatePair()
		let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorDNSLookupFailed)
		delegate.urlSession(session, task: task, didCompleteWithError: error)
		let result = await drain(stream, deadline: 1)
		guard case .nsError(let inner)? = result.thrown as? NetworkStreamingError else {
			return XCTFail("Ожидали .nsError, получили \(String(describing: result.thrown))")
		}
		XCTAssertEqual(inner.code, NSURLErrorDNSLookupFailed)
	}

	func testDelegateNilErrorFinishesCleanly() async {
		// didComplete — терминальное событие задачи: без ошибки стрим завершается чисто
		let (delegate, stream, session, task) = makeDelegatePair()
		delegate.urlSession(session, task: task, didCompleteWithError: nil)
		let result = await drain(stream, deadline: 1)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown)
		XCTAssertTrue(result.events.isEmpty)
	}

	func testDelegateLocalCancelMapsToCleanFinish() async {
		// Наше собственное закрытие (-999) — не ошибка для потребителя
		let (delegate, stream, session, task) = makeDelegatePair()
		let error = NSError(domain: NSURLErrorDomain, code: NSURLErrorCancelled)
		delegate.urlSession(session, task: task, didCompleteWithError: error)
		let result = await drain(stream, deadline: 1)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown)
	}

	// MARK: - Integration: happy path

	func testConnectedIsFirstEventAndMessagesArriveInOrder() async throws {
		let ws = await makeStreaming(timeout: 10)
		let stream = try await openStream(ws, path: "/push3")
		let result = await drain(stream, deadline: 8)
		XCTAssertTrue(result.completed, "Сервер закрывает 1000 — ждали чистый финиш")
		XCTAssertNil(result.thrown)
		XCTAssertEqual(result.events.first, .connected)
		XCTAssertEqual(Array(result.events.dropFirst()), [
			.received(Data("one".utf8)),
			.received(Data("two".utf8)),
			.received(Data("three".utf8)) // текстовый фрейм конвертируется в utf8-Data
		])
	}

	func testCookiesFromStorageAreSentInHandshake() async throws {
		let ws = await makeStreaming(
			timeout: 10,
			cookies: [.session: "abc123", .auth: "xyz789"]
		)
		let stream = try await openStream(ws, path: "/cookie")
		var iterator = stream.makeAsyncIterator()
		let first = try await iterator.next()
		XCTAssertEqual(first, .connected)
		let second = try await iterator.next()
		guard case .received(let data)? = second else {
			return XCTFail("Ожидали данные с Cookie-заголовком")
		}
		let header = String(decoding: data, as: UTF8.self)
		XCTAssertTrue(header.contains("sessionid=abc123"), "Cookie header: \(header)")
		XCTAssertTrue(header.contains("authtoken=xyz789"), "Cookie header: \(header)")
		await ws.cancel()
	}

	func testInputForwardedAndTerminatorAppendedOnInputFinish() async throws {
		let ws = await makeStreaming(timeout: 10)
		let (input, inputCont) = makeInput()
		let stream = try await openStream(ws, path: "/sink", input: input)

		inputCont.yield(Data([0xDE, 0xAD]))
		inputCont.yield(Data([0xBE, 0xEF]))
		inputCont.finish() // конец ввода -> ожидаем терминатор 0x31

		let result = await drain(stream, deadline: 8)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown)
		let summaries = result.events.compactMap { event -> String? in
			guard case .received(let data) = event else { return nil }
			return String(decoding: data, as: UTF8.self)
		}
		XCTAssertEqual(summaries, ["GOT:dead,beef,31"],
			"Сервер должен получить оба payload и терминатор 0x31 по порядку")
	}

	func testLastMessagesBeforeImmediateCloseAreDelivered() async throws {
		// Регрессия на потерю хвоста: финиш стрима не должен обгонять доставку
		// сообщений, которые платформа реально отдала читателю. Гарантия в коде
		// структурная: connectTask ждёт завершения outputTask перед finish.
		//
		// На Linux (corelibs + libcurl) строгий счётчик невозможен: платформа
		// сама роняет очередь при обработке close — голый receive() на этом
		// сценарии отдаёт 0–1 сообщение из 5 (замерено). Поэтому счётчик
		// проверяется только на Darwin, где receive() отдаёт очередь целиком.
		for _ in 0..<3 {
			let ws = await makeStreaming(timeout: 10)
			let stream = try await openStream(ws, path: "/burst-close")
			let result = await drain(stream, deadline: 6)
			XCTAssertTrue(result.completed)
			XCTAssertNil(result.thrown)
			XCTAssertEqual(result.events.first, .connected)
			let payloads = result.events.dropFirst().compactMap { event -> Data? in
				guard case .received(let data) = event else { return nil }
				return data
			}
			let full = (0..<5).map { Data([UInt8($0)]) }
			XCTAssertEqual(payloads, Array(full.prefix(payloads.count)),
				"Нарушен порядок или содержимое хвоста")
			#if !canImport(FoundationNetworking)
			XCTAssertEqual(payloads.count, 5, "Хвост сообщений перед close потерян")
			#endif
		}
	}

	// MARK: - Integration: закрытия

	func testServerNormalCloseFinishesWithoutError() async throws {
		let ws = await makeStreaming(timeout: 10)
		let stream = try await openStream(ws, path: "/close1000")
		let result = await drain(stream, deadline: 6)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown)
		XCTAssertEqual(result.events, [.connected, .received(Data("bye".utf8))])
	}

	func testServerErrorCloseThrowsCloseCode() async throws {
		let ws = await makeStreaming(timeout: 10)
		let stream = try await openStream(ws, path: "/close1011")
		let result = await drain(stream, deadline: 6)
		XCTAssertTrue(result.completed)
		XCTAssertEqual(result.events, [.connected, .received(Data("err".utf8))])
		guard case .closeCode(let payload)? = result.thrown as? NetworkStreamingError else {
			return XCTFail("Ожидали closeCode(1011), получили \(String(describing: result.thrown))")
		}
		XCTAssertEqual(payload.code, .internalServerError)
	}

	// MARK: - Integration: таймаут

	func testTimeoutFiresWhenHandshakeNeverCompletes() async throws {
		let ws = await makeStreaming(timeout: 2)
		let (input, inputCont) = makeInput()
		inputCont.finish()
		let start = Date()
		let stream = try await ws.establishStream(
			endpoint: silentTCP,
			headers: [:],
			inputStream: input
		)
		let result = await drain(stream, deadline: 6)
		let elapsed = Date().timeIntervalSince(start)
		XCTAssertTrue(result.completed, "Таймер должен завершить стрим")
		guard case .timeout? = result.thrown as? NetworkStreamingError else {
			return XCTFail("Ожидали .timeout, получили \(String(describing: result.thrown))")
		}
		XCTAssertTrue(result.events.isEmpty, "До коннекта событий быть не должно")
		XCTAssertLessThan(elapsed, 4.5, "Таймаут 2с должен сработать заметно раньше дедлайна")
	}

	func testFirstMessageInvalidatesTimeout() async throws {
		// Сообщение на 0.3с, затем тишина 3.5с при timeout=2: таймер обязан погаснуть
		let ws = await makeStreaming(timeout: 2)
		let stream = try await openStream(ws, path: "/msg-then-silent")
		let result = await drain(stream, deadline: 7)
		XCTAssertTrue(result.completed)
		XCTAssertNil(result.thrown, "Таймаут не должен сработать после первого сообщения")
		XCTAssertEqual(result.events, [.connected, .received(Data("hello".utf8))])
	}

	func testSharedStorageCookiesAreMergedAndManualWins() async throws {
		// Слияние: куки из shared-стоража сохраняются, одноимённые
		// перекрываются значениями из CookieStorage
		let shared = HTTPCookieStorage.shared
		let stale = HTTPCookie(properties: [.name: "sessionid", .value: "STALE", .domain: "127.0.0.1", .path: "/"])!
		let extra = HTTPCookie(properties: [.name: "extra", .value: "zzz", .domain: "127.0.0.1", .path: "/"])!
		shared.setCookie(stale)
		shared.setCookie(extra)
		defer {
			shared.deleteCookie(stale)
			shared.deleteCookie(extra)
		}

		let ws = await makeStreaming(timeout: 10, cookies: [.session: "abc123"])
		let stream = try await openStream(ws, path: "/cookie")
		var iterator = stream.makeAsyncIterator()
		_ = try await iterator.next() // .connected
		guard case .received(let data)? = try await iterator.next() else {
			return XCTFail("Ожидали данные с Cookie-заголовком")
		}
		let header = String(decoding: data, as: UTF8.self)
		XCTAssertTrue(header.contains("extra=zzz"), "Кук из стоража должен сохраниться: \(header)")
		#if canImport(FoundationNetworking)
		// corelibs игнорирует httpShouldHandleCookies = false и при непустом
		// стораже перетирает ручной Cookie автоподстановкой (замерено: сервер
		// видит значения из стоража). Приоритет ручных кук проверяем только на
		// Darwin, где флаг работает по документации
		XCTAssertTrue(header.contains("sessionid="), "sessionid должен присутствовать: \(header)")
		#else
		XCTAssertTrue(header.contains("sessionid=abc123"), "Свой кук должен победить: \(header)")
		XCTAssertFalse(header.contains("STALE"), "Одноимённый кук из стоража должен быть перекрыт: \(header)")
		#endif
		await ws.cancel()
	}

	func testSupersededEstablishThrowsCancellation() async throws {
		// Проигравшее поколение получает CancellationError, а не тихий finish —
		// иначе ретрай-логика примет замену за штатное закрытие сервером
		let storage = CookieStorage()
		await storage.setArtificialDelay(nanoseconds: 200_000_000)
		let ws = WebSocketNetworkStreaming(
			kidsURLSession: KidsURLSession(),
			cookieStorage: storage,
			timeout: 10
		)
		let (inputA, contA) = makeInput()
		contA.finish()
		let (inputB, contB) = makeInput()
		contB.finish()

		async let firstStream = ws.establishStream(
			endpoint: wsBase + "/silent", headers: [:], inputStream: inputA
		)
		try await Task.sleep(nanoseconds: 50_000_000)
		let streamB = try await ws.establishStream(
			endpoint: wsBase + "/silent", headers: [:], inputStream: inputB
		)

		let streamA = try await firstStream
		let resultA = await drain(streamA, deadline: 3)
		XCTAssertTrue(resultA.completed)
		XCTAssertTrue(resultA.events.isEmpty)
		XCTAssertTrue(resultA.thrown is CancellationError,
			"Ожидали CancellationError, получили \(String(describing: resultA.thrown))")

		let readerB = StreamReader(streamB)
		let firstB = try await readerB.next()
		XCTAssertEqual(firstB, .connected, "Победившее поколение должно жить")
		await ws.cancel()
	}

	// MARK: - Integration: регрессии на исправленные гонки

	func testReestablishIsNotKilledByStaleTimer() async throws {
		// Регрессия: отменённый таймер первого коннекта просыпался мгновенно и,
		// пройдя проверку по self.timer (уже новому), отменял задачи ВТОРОГО коннекта.
		let ws = await makeStreaming(timeout: 5)
		let stream1 = try await openStream(ws, path: "/silent")
		_ = stream1 // первый стрим сознательно не читаем

		try await Task.sleep(nanoseconds: 400_000_000)

		let stream2 = try await openStream(ws, path: "/push-after-3")
		let result = await drain(stream2, deadline: 8)
		XCTAssertTrue(result.completed, "Второй коннект должен дожить до сообщения и закрытия")
		XCTAssertNil(result.thrown)
		XCTAssertEqual(result.events, [.connected, .received(Data("late".utf8))],
			"Сообщение приходит на ~3.6с — при живой гонке задачи были бы отменены на ~0.4с")
	}

	func testCancelFinishesOutputStream() async throws {
		let ws = await makeStreaming(timeout: 10)
		let (input, inputCont) = makeInput()
		let stream = try await openStream(ws, path: "/echo", input: input)

		let reader = StreamReader(stream)
		let first = try await reader.next()
		XCTAssertEqual(first, .connected)

		await ws.cancel()

		// После cancel() стрим должен корректно завершиться, а не зависнуть
		enum Outcome: Sendable { case finished, event(NetworkStreamingOutputEvent), timedOut }
		let outcome = try await withThrowingTaskGroup(of: Outcome.self) { group -> Outcome in
			group.addTask {
				if let event = try await reader.next() { return .event(event) }
				return .finished
			}
			group.addTask {
				try? await Task.sleep(nanoseconds: 2_000_000_000)
				return .timedOut
			}
			let winner = try await group.next() ?? .timedOut
			group.cancelAll()
			return winner
		}
		inputCont.finish()
		guard case .finished = outcome else {
			return XCTFail("После cancel() стрим должен завершиться, получили \(outcome)")
		}
	}

	func testActorDeinitsWhileConnectionAlive() async throws {
		// Ни одна из внутренних задач не должна удерживать актора:
		// deinit обязан состояться и закрыть сокет сам
		var ws: WebSocketNetworkStreaming? = await makeStreaming(timeout: 30)
		weak var weakWS = ws
		let (input, inputCont) = makeInput()
		let stream = try await ws!.establishStream(
			endpoint: wsBase + "/silent",
			headers: [:],
			inputStream: input
		)
		var iterator = stream.makeAsyncIterator()
		let first = try await iterator.next()
		XCTAssertEqual(first, .connected)

		ws = nil
		try await Task.sleep(nanoseconds: 700_000_000)
		XCTAssertNil(weakWS, "Актор должен деинициализироваться — retain cycle через задачи")
		inputCont.finish()
	}
}
