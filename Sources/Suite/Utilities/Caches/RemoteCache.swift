//
//  RemoteCache.swift
//
//  Created by ben on 1/7/21.
//  Updated to make inflightRequests thread-safe via a serial queue.
//

import Foundation

#if canImport(Combine)
import Combine

@available(OSX 10.15, iOS 13.0, tvOS 13, watchOS 6, *)
public protocol RemoteCacheRequestBuilder {
    func request(from url: URL) -> AnyPublisher<URLRequest, Error>
}

public protocol AsyncRemoteCacheRequestBuilder {
    func request(from url: URL) async throws -> URLRequest
}

@available(OSX 10.15, iOS 13.0, tvOS 13, watchOS 6, *)
public class RemoteCache<Element: Cachable>: Cache<Element> {

    let session: URLSession
    let requestBuilder: RemoteCacheRequestBuilder?

    // Thread-safe access via inflightQueue
    private var inflightRequests: [URL: AnyPublisher<Element, Error>] = [:]
    private let inflightQueue = DispatchQueue(label: "RemoteCache.inflightRequests")

    public init(session urlSession: URLSession = .shared,
                requestBuilder builder: RemoteCacheRequestBuilder? = nil) {
        session = urlSession
        requestBuilder = builder
        super.init(backingCache: nil)
    }

    public override func cachedValue(for url: URL, newerThan date: Date? = nil) -> Element? {
        nil
    }

    // MARK: - Combine-based fetch

    public override func fetch(for url: URL,
                               caching: URLRequest.CachePolicy = .default) -> AnyPublisher<Element, Error> {
        // Respect "cache only" behavior
        if caching == .returnCacheDataDontLoad {
            return Fail(outputType: Element.self,
                        failure: CacheError.noLocalItemFound(url))
                .eraseToAnyPublisher()
        }

        // If we already have an in-flight request for this URL, return it
        if caching != .reloadIgnoringLocalCacheData {
            if let inflight = inflightQueue.sync(execute: { inflightRequests[url] }) {
                return inflight
            }
        }

        // If there's a builder, let it construct the request first
        if let builder = requestBuilder {
            return builder.request(from: url)
                .flatMap { [weak self] request -> AnyPublisher<Element, Error> in
                    guard let self = self else {
                        return Fail(outputType: Element.self,
                                    failure: CacheError.failedToDownload(url, Data()))
                            .eraseToAnyPublisher()
                    }
                    return self.publisher(for: request)
                }
                .eraseToAnyPublisher()
        }

        return publisher(for: URLRequest(url: url))
    }

    // MARK: - async/await fetch

    public override func fetch(for url: URL,
                               caching: URLRequest.CachePolicy = .default) async throws -> Element {
        if caching == .returnCacheDataDontLoad {
            throw CacheError.noLocalItemFound(url)
        }

        var request = URLRequest(url: url)
        if let builder = requestBuilder as? AsyncRemoteCacheRequestBuilder {
            request = try await builder.request(from: url)
        }

        let (data, _) = try await session.data(for: request)
        if let downloaded = Element.create(with: data) as? Element {
            return downloaded
        }
        throw CacheError.failedToDownload(url, data)
    }

    // MARK: - Internal publisher

    func publisher(for request: URLRequest) -> AnyPublisher<Element, Error> {
        guard let url = request.url else {
            return Fail(outputType: Element.self,
                        failure: CacheError.noURL)
                .eraseToAnyPublisher()
        }

        let pub: AnyPublisher<Element, Error> = session.dataTaskPublisher(for: request)
            .assumeHTTP()
            .mapError { [weak self] error -> Error in
                // Clear inflight entry on error, on the inflightQueue
                self?.inflightQueue.async {
                    self?.inflightRequests.removeValue(forKey: url)
                }
                return CacheError.failedToDownloadServerError(request.url ?? url, error)
            }
            .tryMap { [weak self] data -> Element in
                // Clear inflight entry on success, on the inflightQueue
                self?.inflightQueue.async {
                    self?.inflightRequests.removeValue(forKey: url)
                }

                if let result = Element.create(with: data.data) as? Element {
                    return result
                }
                throw CacheError.failedToDownload(request.url ?? url, data.data)
            }
            .eraseToAnyPublisher()

        // Store the in-flight publisher in a thread-safe way
        inflightQueue.async {
            self.inflightRequests[url] = pub
        }

        return pub
    }
}

// MARK: - Helper extension

@available(iOS 13.0, watchOS 6.0, OSX 10.15, *)
fileprivate extension Publisher where Output == (data: Data, response: URLResponse) {
    func assumeHTTP() -> AnyPublisher<(data: Data, response: HTTPURLResponse), Error> {
        tryMap { data, response in
            guard let http = response as? HTTPURLResponse else {
                throw CacheError.unknownResponse(response.url)
            }
            return (data, http)
        }
        .eraseToAnyPublisher()
    }
}

#endif
