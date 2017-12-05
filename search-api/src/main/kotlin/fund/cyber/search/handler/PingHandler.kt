package fund.cyber.search.handler

import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange

class PingHandler : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {
        exchange.statusCode = 200
    }
}