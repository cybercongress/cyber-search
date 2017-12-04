package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.node.common.longValue
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers

class EthereumBlockHandler(
        private val ethereumDaoService: EthereumDaoService = AppContext.ethereumDaoService,
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer
) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val blockNumber = exchange.queryParameters["blockNumber"]?.longValue()

        if (blockNumber == null) {
            exchange.statusCode = 400
            return
        }

        val block = ethereumDaoService.getBlockByNumber(blockNumber)

        if (block == null) {
            exchange.statusCode = 404
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString(block)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}