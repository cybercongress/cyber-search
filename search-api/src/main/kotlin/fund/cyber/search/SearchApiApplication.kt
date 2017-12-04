package fund.cyber.search

import fund.cyber.search.configuration.SearchApiConfiguration
import fund.cyber.search.handler.*
import io.undertow.Handlers
import io.undertow.Undertow


object SearchApiApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val httpHandler = Handlers.routing()
                .get("/search", SearchHandler())
                .get("/bitcoin/block/{blockNumber}", BitcoinBlockHandler())
                .get("/bitcoin/tx/{txId}", BitcoinTxHandler())
                .get("/bitcoin/address/{address}", BitcoinAddressHandler())
                .get("/ethereum/block/{blockNumber}", EthereumBlockHandler())
                .get("/ethereum/tx/{txHash}", EthereumTxHandler())

        val setCorsHeaderHandler = SetCorsHeadersHandler(httpHandler, SearchApiConfiguration.allowedCORS)

        Undertow.builder()
                .addHttpListener(10300, "0.0.0.0")
                .setHandler(setCorsHeaderHandler)
                .build().start()
    }
}