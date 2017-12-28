package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.common.stringValue
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers


class EthereumTxHandler(
        repository: CassandraKeyspaceRepository,
        private val jsonSerializer: ObjectMapper = AppContext.getJsonSerializer()
) : HttpHandler {

    private val txTable = repository.mappingManager.mapper(EthereumTransaction::class.java)

    override fun handleRequest(exchange: HttpServerExchange) {

        val txHash = exchange.queryParameters["txHash"]?.stringValue()

        if (txHash == null || txHash.isEmpty()) {
            exchange.statusCode = 400
            return
        }

        val tx = txTable.get(txHash)

        if (tx == null) {
            exchange.statusCode = 404
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString(tx)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}