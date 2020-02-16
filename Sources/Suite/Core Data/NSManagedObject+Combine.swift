//
//  File.swift
//  
//
//  Created by Ben Gottlieb on 2/16/20.
//

import CoreData
import Combine
import SwiftUI

public protocol CoreDataFieldType {
	static var defaultValue: Self { get }
}

extension Bool: CoreDataFieldType {
	public static var defaultValue: Bool { return false }
}

extension String: CoreDataFieldType {
	public static var defaultValue: String { return "" }
}

extension Double: CoreDataFieldType {
	public static var defaultValue: Double { return 0.0 }
}

extension Int: CoreDataFieldType {
	public static var defaultValue: Int { return 0 }
}

@available(iOS 13.0, *)
public extension NSManagedObject {
	func binding<T: CoreDataFieldType>(for field: String, defaultValue: T = T.defaultValue) -> Binding<T> {
		return Binding<T>(get: { return self.value(forKey: field) as? T ?? defaultValue }, set:
			{ newValue in self.setValue(newValue, forKey: field)} )
	}

	func binding<T: NSManagedObject>(for field: String) -> Binding<T?> {
		return Binding<T?>(get: { return self.value(forKey: field) as? T }, set:
			{ newValue in self.setValue(newValue, forKey: field)} )
	}
}
