import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

// Заглушки внешних типов AssistantSDK — только для тестового окружения.
// Сигнатуры сверены с использованием в WebSocketNetworkStreaming.

enum AssistantSDK {
	enum NetworkStreamingOutputEvent: Equatable {
		case connected
		case received(Data)
	}
}

typealias NetworkStreamingOutputEvent = AssistantSDK.NetworkStreamingOutputEvent

protocol NetworkStreaming {
	func establishStream(
		endpoint: String,
		headers: [String: String],
		inputStream: AsyncStream<Data>
	) async throws -> AsyncThrowingStream<AssistantSDK.NetworkStreamingOutputEvent, any Error>
	func cancel() async
}

struct NetworkStreamingOutputError {
	let code: URLSessionWebSocketTask.CloseCode
}

enum NetworkStreamingError: Error {
	case timeout
	case closeCode(NetworkStreamingOutputError)
	case nsError(NSError)
}

/// Тестовый двойник: в проде это делегат сессии с challenge-логикой.
final class KidsURLSession: NSObject, URLSessionDelegate {}

#if canImport(FoundationNetworking)
// Linux-шим: swift-corelibs-foundation доставляет WebSocket-события только
// делегату СЕССИИ и игнорирует task.delegate (на Darwin — наоборот, task.delegate
// имеет приоритет). Здесь эмулируем Darwin-диспатч: форвардим события сессии
// в task.delegate, чтобы продакшен-код тестировался без изменений.
// ВАЖНО: это CI-леса, а не поддержка Linux. Без шима прод-путь на Linux
// мёртв (.connected не придёт, стрим отвалится по таймауту) — зелёные
// тесты здесь не означают «работает на Linux».
extension KidsURLSession: URLSessionWebSocketDelegate, URLSessionTaskDelegate {

	func urlSession(
		_ session: URLSession,
		webSocketTask: URLSessionWebSocketTask,
		didOpenWithProtocol proto: String?
	) {
		(webSocketTask.delegate as? URLSessionWebSocketDelegate)?
			.urlSession(session, webSocketTask: webSocketTask, didOpenWithProtocol: proto)
	}

	func urlSession(
		_ session: URLSession,
		webSocketTask: URLSessionWebSocketTask,
		didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
		reason: Data?
	) {
		(webSocketTask.delegate as? URLSessionWebSocketDelegate)?
			.urlSession(session, webSocketTask: webSocketTask, didCloseWith: closeCode, reason: reason)
	}

	func urlSession(
		_ session: URLSession,
		task: URLSessionTask,
		didCompleteWithError error: Error?
	) {
		task.delegate?
			.urlSession(session, task: task, didCompleteWithError: error)
	}

	// ВНИМАНИЕ: форвардеры пишутся руками, обобщённого механизма нет. Новый
	// метод URLSessionTaskDelegate в проде без парного форвардера здесь на
	// Linux не вызовется вовсе — и молча
	func urlSession(
		_ session: URLSession,
		task: URLSessionTask,
		willPerformHTTPRedirection response: HTTPURLResponse,
		newRequest request: URLRequest,
		completionHandler: @Sendable @escaping (URLRequest?) -> Void
	) {
		guard let delegate = task.delegate else {
			completionHandler(request)
			return
		}
		delegate.urlSession(
			session,
			task: task,
			willPerformHTTPRedirection: response,
			newRequest: request,
			completionHandler: completionHandler
		)
	}
}
#endif

actor CookieStorage {
	private var store: [String: String] = [:]
	private var artificialDelayNanoseconds: UInt64 = 0

	func set(_ value: String, for name: String) {
		store[name] = value
	}

	func setArtificialDelay(nanoseconds: UInt64) {
		artificialDelayNanoseconds = nanoseconds
	}

	func getCookie(name: String) async -> String? {
		if artificialDelayNanoseconds > 0 {
			try? await Task.sleep(nanoseconds: artificialDelayNanoseconds)
		}
		return store[name]
	}
}

enum Cookies: String, CaseIterable {
	case session = "sessionid"
	case auth = "authtoken"
}

struct AssistantLogger {
	func info(_ message: String) {}
	func error(_ message: String) {}
}

enum Logger {
	static let assistant = AssistantLogger()
}

func S(_ string: String) -> String { string }
