package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.common.longValue
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers


class BitcoinBlockHandler(
        repository: CassandraKeyspaceRepository,
        private val jsonSerializer: ObjectMapper = AppContext.getJsonSerializer()
) : HttpHandler {

    private val blockTable = repository.mappingManager.mapper(BitcoinBlock::class.java)

    override fun handleRequest(exchange: HttpServerExchange) {

        val blockNumber = exchange.queryParameters["blockNumber"]?.longValue()

        if (blockNumber == null) {
            exchange.statusCode = 400
            return
        }

        val block = blockTable.get(blockNumber)

        if (block == null) {
            exchange.statusCode = 404
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString(block)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}
