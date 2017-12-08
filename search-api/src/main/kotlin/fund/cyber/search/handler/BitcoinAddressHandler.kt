package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.common.stringValue
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers

class BitcoinAddressHandler(
        repository: CassandraKeyspaceRepository,
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer
) : HttpHandler {


    private val addressTable = repository.mappingManager.mapper(BitcoinAddress::class.java)

    override fun handleRequest(exchange: HttpServerExchange) {

        val address = exchange.queryParameters["address"]?.stringValue()

        if (address == null) {
            exchange.statusCode = 400
            return
        }

        val addressInformation = addressTable.get(address)

        if (addressInformation == null) {
            exchange.statusCode = 404
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString(addressInformation)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}