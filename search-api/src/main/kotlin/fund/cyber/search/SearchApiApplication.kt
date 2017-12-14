package fund.cyber.search

import fund.cyber.cassandra.model.ChainsIndex.INDEX_TO_CHAIN_ENTITY
import fund.cyber.search.configuration.AppContext
import fund.cyber.search.configuration.SearchApiConfiguration
import fund.cyber.search.handler.*
import io.undertow.Handlers
import io.undertow.Undertow


object SearchApiApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val bitcoinRepository = AppContext.cassandraService.bitcoinRepository
        val bitcoinCashRepository = AppContext.cassandraService.bitcoinCashRepository
        val ethereumRepository = AppContext.cassandraService.ethereumRepository
        val ethereumClassicRepository = AppContext.cassandraService.ethereumClassicRepository

        val httpHandler = Handlers.routing()
                .get("/index-stats", IndexStatusHandler(indexToChainEntity = INDEX_TO_CHAIN_ENTITY))
                .get("/search", SearchHandler(indexToChainEntity = INDEX_TO_CHAIN_ENTITY))
                .get("/ping", PingHandler())


                .get("/bitcoin/block/{blockNumber}", BitcoinBlockHandler(bitcoinRepository))
                .get("/bitcoin/block/{blockNumber}/transactions", BitcoinBlockTxHandler(bitcoinRepository))
                .get("/bitcoin/tx/{txHash}", BitcoinTxHandler(bitcoinRepository))
                .get("/bitcoin/address/{address}", BitcoinAddressHandler(bitcoinRepository))
                .get("/bitcoin/address/{address}/transactions", BitcoinAddressTxHandler(bitcoinRepository))

                .get("/bitcoin_cash/block/{blockNumber}", BitcoinBlockHandler(bitcoinCashRepository))
                .get("/bitcoin_cash/block/{blockNumber}/transactions", BitcoinBlockTxHandler(bitcoinCashRepository))
                .get("/bitcoin_cash/tx/{txHash}", BitcoinTxHandler(bitcoinCashRepository))
                .get("/bitcoin_cash/address/{address}", BitcoinAddressHandler(bitcoinCashRepository))
                .get("/bitcoin_cash/address/{address}/transactions", BitcoinAddressTxHandler(bitcoinCashRepository))


                .get("/ethereum/block/{blockNumber}", EthereumBlockHandler(ethereumRepository))
                .get("/ethereum/block/{blockNumber}/transactions", EthereumBlockTxHandler(ethereumRepository))
                .get("/ethereum/tx/{txHash}", EthereumTxHandler(ethereumRepository))
                .get("/ethereum/address/{address}", EthereumAddressHandler(ethereumRepository))
                .get("/ethereum/address/{address}/transactions", EthereumAddressTxHandler(ethereumRepository))

                .get("/ethereum_classic/block/{blockNumber}", EthereumBlockHandler(ethereumClassicRepository))
                .get("/ethereum_classic/block/{blockNumber}/transactions", EthereumBlockTxHandler(ethereumClassicRepository))
                .get("/ethereum_classic/tx/{txHash}", EthereumTxHandler(ethereumClassicRepository))
                .get("/ethereum_classic/address/{address}", EthereumAddressHandler(ethereumClassicRepository))
                .get("/ethereum_classic/address/{address}/transactions", EthereumAddressTxHandler(ethereumClassicRepository))


        val setCorsHeaderHandler = SetCorsHeadersHandler(httpHandler, SearchApiConfiguration.allowedCORS)

        Undertow.builder()
                .addHttpListener(10300, "0.0.0.0")
                .setHandler(setCorsHeaderHandler)
                .build().start()
    }
}