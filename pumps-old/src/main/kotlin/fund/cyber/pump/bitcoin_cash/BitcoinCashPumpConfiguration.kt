package fund.cyber.pump.bitcoin_cash

import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.common.env
import fund.cyber.node.model.JsonRpcBitcoinTransaction
import fund.cyber.pump.PumpsContext
import fund.cyber.pump.bitcoin.BitcoinJsonRpcClient
import fund.cyber.pump.bitcoin.JsonRpcBlockToBitcoinBundleConverter
import fund.cyber.pump.bitcoin.JsonRpcToDaoBitcoinBlockConverter
import fund.cyber.pump.bitcoin.JsonRpcToDaoBitcoinTransactionConverter


object BitcoinCashPumpConfiguration {
    val abcUrl: String = env("ABC_URL", "http://fund.cyber:fund.cyber@127.0.0.1:7332")
}


object BitcoinCashPumpContext {

    private val txCache = PumpsContext.cacheManager
            .getCache("bitcoin_cash.transactions", String::class.java, JsonRpcBitcoinTransaction::class.java)


    val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinJsonRpcClient(
            PumpsContext.jacksonJsonSerializer, PumpsContext.jacksonJsonDeserializer,
            PumpsContext.httpClient, BitcoinCashPumpConfiguration.abcUrl
    )

    private val jsonRpcToDaoBitcoinTransactionConverter = JsonRpcToDaoBitcoinTransactionConverter()
    private val jsonRpcToDaoBitcoinBlockConverter = JsonRpcToDaoBitcoinBlockConverter()

    val rpcToBundleEntitiesConverter = JsonRpcBlockToBitcoinBundleConverter(
            BITCOIN_CASH, bitcoinJsonRpcClient, txCache,
            jsonRpcToDaoBitcoinTransactionConverter, jsonRpcToDaoBitcoinBlockConverter
    )
}
