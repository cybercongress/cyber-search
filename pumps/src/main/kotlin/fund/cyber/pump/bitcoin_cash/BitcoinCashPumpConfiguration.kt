package fund.cyber.pump.bitcoin_cash

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.common.env
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.PumpsContext
import fund.cyber.pump.bitcoin.*


object BitcoinCashPumpConfiguration {
    val abcUrl: String = env("ABC_URL", "http://bitcoin:password@127.0.0.1:18332")
}


object BitcoinCashPumpContext {

    val txCache = PumpsContext.cacheManager
            .getCache("bitcoin_cash.transactions", String::class.java, BitcoinTransaction::class.java)



    val bitcoinJsonRpcClient: BitcoinJsonRpcClient = BitcoinJsonRpcClient(
            PumpsContext.jacksonJsonSerializer, PumpsContext.jacksonJsonDeserializer,
            PumpsContext.httpClient, BitcoinCashPumpConfiguration.abcUrl
    )


    val bitcoinDaoService = BitcoinDaoService(PumpsContext.cassandra, Chain.BITCOIN, txCache)

    private val jsonRpcToDaoBitcoinTransactionConverter = JsonRpcToDaoBitcoinTransactionConverter()
    private val jsonRpcToDaoBitcoinBlockConverter = JsonRpcToDaoBitcoinBlockConverter()

    val jsonRpcToDaoBitcoinEntitiesConverter = JsonRpcBlockToBitcoinBundleConverter(
            BITCOIN_CASH, bitcoinDaoService, jsonRpcToDaoBitcoinTransactionConverter, jsonRpcToDaoBitcoinBlockConverter
    )
}