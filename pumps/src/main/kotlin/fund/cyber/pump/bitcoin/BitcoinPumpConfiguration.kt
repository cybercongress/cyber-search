package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.Chain
import fund.cyber.node.common.env
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.PumpsContext


object BitcoinPumpConfiguration {
    val btcdUrl: String = env("BTCD_URL", "http://cyber:cyber@127.0.0.1:8334")
}


object BitcoinPumpContext {

    val txCache = PumpsContext.cacheManager
            .getCache("bitcoin.transactions", String::class.java, BitcoinTransaction::class.java)


    val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinJsonRpcClient(
            PumpsContext.jacksonJsonSerializer, PumpsContext.jacksonJsonDeserializer,
            PumpsContext.httpClient, BitcoinPumpConfiguration.btcdUrl
    )

    val bitcoinDaoService = BitcoinDaoService(PumpsContext.cassandra, Chain.BITCOIN, txCache)

    private val jsonRpcToDaoBitcoinTransactionConverter = JsonRpcToDaoBitcoinTransactionConverter()
    private val jsonRpcToDaoBitcoinBlockConverter = JsonRpcToDaoBitcoinBlockConverter()

    val jsonRpcToDaoBitcoinEntitiesConverter = JsonRpcBlockToBitcoinBundleConverter(
            bitcoinDaoService, jsonRpcToDaoBitcoinTransactionConverter, jsonRpcToDaoBitcoinBlockConverter
    )
}