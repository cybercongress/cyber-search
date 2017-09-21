package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.common.longValue
import fund.cyber.search.context.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers

class BitcoinBlockHandler(
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer
) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val blockNumber = exchange.pathParameters["blockNumber"]?.longValue()

        if (blockNumber == null || blockNumber < 0) {
            exchange.statusCode = 400
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString("""{"block":"$blockNumber"}""")

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}
