// swift-tools-version:5.9
import PackageDescription

let package = Package(
	name: "WSKit",
	platforms: [
		.macOS(.v12)
	],
	targets: [
		.target(name: "WSKit", path: "Sources/WSKit"),
		.testTarget(name: "WSKitTests", dependencies: ["WSKit"], path: "Tests/WSKitTests")
	]
)
