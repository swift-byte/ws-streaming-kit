// swift-tools-version:6.0
import PackageDescription

let package = Package(
	name: "WSKit",
	platforms: [
		.iOS(.v15),
		.macOS(.v12)
	],
	targets: [
		.target(name: "WSKit", path: "Sources/WSKit"),
		.testTarget(name: "WSKitTests", dependencies: ["WSKit"], path: "Tests/WSKitTests")
	]
)
