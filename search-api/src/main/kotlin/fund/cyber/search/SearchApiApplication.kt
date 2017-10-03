package fund.cyber.search

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
                .get("/ethereum/block/{blockNumber}", EthereumBlockHandler())
                .get("/ethereum/tx/{txHash}", EthereumTxHandler())

        Undertow.builder()
                .addHttpListener(8085, "0.0.0.0")
                .setHandler(httpHandler)
                .build().start()
    }
}