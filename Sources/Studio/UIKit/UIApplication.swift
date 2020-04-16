//
//  UIApplication.swift
//  
//
//  Created by ben on 12/14/19.
//

#if canImport(UIKit)

import UIKit

@available(iOS 13.0, *)
public extension UIApplication {
	var currentScene: UIWindowScene? {
		self.connectedScenes
			.filter { $0.activationState == .foregroundActive }
			.compactMap { $0 as? UIWindowScene }
			.first
	}
}

public extension UIApplication {
    var currentWindow: UIWindow? {
		if #available(iOS 13.0, *) {
			if let window = self.currentScene?.frontWindow { return window }
		}
		
		if let window = self.delegate?.window { return window }
		return self.windows.first
    }

}

#endif

