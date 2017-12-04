package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.stringValue
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers

class BitcoinAddressHandler(
        private val bitcoinDaoService: BitcoinDaoService = AppContext.bitcoinDaoService,
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer
) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val address = exchange.queryParameters["address"]?.stringValue()

        if (address == null) {
            exchange.statusCode = 400
            return
        }

        val addressInformation = bitcoinDaoService.getAddress(address)

        if(addressInformation == null) {
            exchange.statusCode = 404
            return
        }

        val rawResponse = jsonSerializer.writeValueAsString(addressInformation)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}