//
//  TimeInterval.swift
//  
//
//  Created by Ben Gottlieb on 12/20/20.
//

import Foundation

public extension TimeInterval {
	static let minute: TimeInterval = 60.0
	static let hour: TimeInterval = 60.0 * 60.0
	static let day: TimeInterval = 60.0 * 60.0 * 24.0
	
	static var saveInterval: TimeInterval = 20.0
	static var keyPressSearchDelay: TimeInterval = 0.5
	static var pressAndHoldInterval: TimeInterval = 1.0

	
	var days: Int { return abs(Int(self / .day)) }
	var hours: Int { return abs(Int(self / .hour)) }
	var minutes: Int { return abs(Int(self / .minute)) }
	var seconds: Int { return abs(Int(self)) }

	var leftoverHours: Int { return Int(self / .hour) % 24 }
	var leftoverMinutes: Int { return Int(self / .minute) % 60 }
	var leftoverSeconds: Int { return Int(self) % 60 }

	enum DurationStyle { case hours, minutes, secondsNoHours, seconds, centiseconds, milliseconds }
	
	func durationString(style: DurationStyle = .seconds, showLeadingZero: Bool = false, roundUp: Bool = true) -> String {
		if roundUp {
			return self.rounded(.up).durationString(style: style, showLeadingZero: showLeadingZero, roundUp: false)
		}

		var leading = showLeadingZero ? "%02d" : "%d"
		if self < 0 { leading = "-" + leading }

		switch style {
		case .hours:
			return String(format: leading, hours)
			
		case .minutes:
			return String(format: leading + ":%02d", hours, minutes % 60)

		case .secondsNoHours:
			return String(format: leading + ":%02d", minutes, seconds % 60)

		case .seconds:
			if hours > 0 { return String(format: leading + ":%02d:%02d", hours, minutes % 60, seconds % 60) }
			return String(format: leading + ":%02d", minutes % 60, seconds % 60)

		case .centiseconds:
			if hours > 0 { return String(format: leading + ":%02d:%02d.%02d", hours, minutes % 60, seconds % 60, Int(self * 100) % 100) }
			return String(format: leading + ":%02d.%02d", minutes % 60, seconds % 60, Int(self * 100) % 100)

		case .milliseconds:
			if hours > 0 { return String(format: leading + ":%02d:%02d.%02d", hours, minutes % 60, seconds % 60, Int(self * 1000) % 1000) }
			return String(format: leading + "%02d.%02d", minutes % 60, seconds % 60, Int(self * 1000) % 1000)

		}
	}
	
	enum DurationAbbreviation { case none, short, veryShort
		var hour: String {
			switch self {
			case .none: return "hour"
			case .short: return "hr"
			case .veryShort: return "h"
			}
		}

		var minute: String {
			switch self {
			case .none: return "minute"
			case .short: return "min"
			case .veryShort: return "m"
			}
		}

		var second: String {
			switch self {
			case .none: return "second"
			case .short: return "sec"
			case .veryShort: return "s"
			}
		}
		
		var separator: String {
			switch self {
			case .none: return ", "
			default: return " "
			}
		}
		
		func pluralize(count: Int, unit: String) -> String {
			switch self {
			case .veryShort:
				return "\(count)\(unit)"
				
			default:
				return Pluralizer.instance.pluralize(count, unit)
			}
		}
	}
	func durationWords(includingSeconds: Bool = true, abbreviated: DurationAbbreviation = .none) -> String {
		var components: [String] = []
		
		if hours != 0 { components.append(abbreviated.pluralize(count: hours, unit: abbreviated.hour)) }
		if leftoverMinutes != 0 { components.append(abbreviated.pluralize(count: leftoverMinutes, unit: abbreviated.minute)) }
		if includingSeconds, leftoverSeconds != 0 { components.append(abbreviated.pluralize(count: leftoverSeconds, unit: abbreviated.second)) }
		
		return components.joined(separator: abbreviated.separator)
	}
	
	init?(string: String?) {
		guard let string = string else { return nil }
		let comps = string.components(separatedBy: ":")
		
		switch comps.count {
		case 0:
			self = 0
			return nil
			
		case 1:
			guard let secs = Double(comps[0]) else {
				self = 0
				return nil
			}
			self = TimeInterval(secs)
			
		case 2:
			guard let mins = Int(comps[0]), let secs = Double(comps[1]) else {
				self = 0
				return nil
			}
			self = TimeInterval(secs + Double(mins) * 60)

		case 3:
			guard let hrs = Int(comps[0]), let mins = Int(comps[1]), let secs = Double(comps[2]) else {
				self = 0
				return nil
			}
			self = TimeInterval(secs + Double(mins) * 60 + Double(hrs) * 3600)
			
		default:
			self = 0
			return nil
		}
	}
}
