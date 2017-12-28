package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.longValue
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers


class EthereumBlockTxHandler(
        repository: EthereumKeyspaceRepository,
        private val jsonSerializer: ObjectMapper = AppContext.getJsonSerializer()
) : HttpHandler {

    private val blockTxTable = repository.ethereumKeyspaceRepositoryAccessor

    override fun handleRequest(exchange: HttpServerExchange) {

        val blockNumber = exchange.queryParameters["blockNumber"]?.longValue()

        if (blockNumber == null) {
            exchange.statusCode = 400
            return
        }

        val addressTxesFuture = blockTxTable.blockTransactions(blockNumber).get()

        val txes = addressTxesFuture?.all() ?: emptyList()

        val rawResponse = jsonSerializer.writeValueAsString(txes)
        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}