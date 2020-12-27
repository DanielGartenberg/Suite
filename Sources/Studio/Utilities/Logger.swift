//
//  Logger.swift
//  
//
//  Created by ben on 12/4/19.
//

import Foundation
import CoreData

public func logg(_ msg: @autoclosure () -> String, _ level: Logger.Level = .mild) { Logger.instance.log(msg(), level: level) }
public func logg(_ special: Logger.Special, _ level: Logger.Level = .mild) { Logger.instance.log(special, level: level) }
public func dlogg(_ msg: @autoclosure () -> String, _ level: Logger.Level = .mild) { Logger.instance.log(msg(), level: level) }
public func logg(error: Error?, _ msg: @autoclosure () -> String, _ level: Logger.Level = .mild) { Logger.instance.log(error: error, msg(), level: level) }
public func dlogg(_ something: Any, _ level: Logger.Level = .mild) { Logger.instance.log("\(something)", level: level) }
public func logg<T>(result: Result<T, Error>, _ msg: @autoclosure () -> String) {
	switch result {
	case .failure(let error): logg(error: error, msg())
	default: break
	}
}

#if canImport(Combine)
import Combine
@available(OSX 10.15, iOS 13.0, tvOS 13, watchOS 6, *)
public func logg<Failure>(completion: Subscribers.Completion<Failure>, _ msg: @autoclosure () -> String) {
	switch completion {
	case .failure(let error): logg(error: error, msg())
	default: break
	}
}
#endif

public class Logger {
	static public let instance = Logger()
	
	private init() { }
	
	public var showTimestamps = true { didSet { self.timestampStart = Date() }}
	public var timestampStart = Date()
	
	public enum Special { case `break` }
	public enum Level: Int, Comparable {
		case off, quiet, mild, loud, verbose
		public static func <(lhs: Level, rhs: Level) -> Bool { return lhs.rawValue < rhs.rawValue }
	}
	
	public var level: Level = {
		if Gestalt.isProductionBuild { return .off }
		if Gestalt.isAttachedToDebugger { return .mild }
		return .quiet
	}()
	
	public func log(_ special: Special, level: Logger.Level = .mild) {
		if level > self.level { return }
		switch special {
		case .break: print("\n")
		}
	}
	
	public func log(_ msg: @autoclosure () -> String, level: Level = .mild) {
		if level > self.level { return }
		var message = msg()
		
		if showTimestamps { message = String(format: "%.04f - ", Date().timeIntervalSince(timestampStart)) + message }
		print(message)
	}
	
	public func log(error: Error?, _ msg: @autoclosure () -> String, level: Level = .mild) {
		if level > self.level { return }
		let message = "⚠️ \(msg()) \(error?.localizedDescription ?? "")"
		print(message)
	}
}

public extension NSManagedObject {
	func logObject(_ level: Logger.Level = .mild) { dlogg("\(self)", level) }
}
