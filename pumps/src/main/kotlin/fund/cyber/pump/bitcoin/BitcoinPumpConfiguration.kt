package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.env
import fund.cyber.node.model.JsonRpcBitcoinTransaction
import fund.cyber.pump.PumpsContext


object BitcoinPumpConfiguration {
    val btcdUrl: String = env("BITCOIND_URL", "http://cyber:cyber@127.0.0.1:8332")
}


object BitcoinPumpContext {

    val txCache = PumpsContext.cacheManager
            .getCache("bitcoin.transactions", String::class.java, JsonRpcBitcoinTransaction::class.java)


    val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinJsonRpcClient(
            PumpsContext.jacksonJsonSerializer, PumpsContext.jacksonJsonDeserializer,
            PumpsContext.httpClient, BitcoinPumpConfiguration.btcdUrl
    )

    val bitcoinDaoService = BitcoinDaoService(PumpsContext.cassandra, BITCOIN)

    private val jsonRpcToDaoBitcoinTransactionConverter = JsonRpcToDaoBitcoinTransactionConverter()
    private val jsonRpcToDaoBitcoinBlockConverter = JsonRpcToDaoBitcoinBlockConverter()

    val jsonRpcToDaoBitcoinEntitiesConverter = JsonRpcBlockToBitcoinBundleConverter(
            BITCOIN, bitcoinJsonRpcClient, txCache,
            jsonRpcToDaoBitcoinTransactionConverter, jsonRpcToDaoBitcoinBlockConverter
    )
}