package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.env
import fund.cyber.pump.PumpsContext


object BitcoinPumpConfiguration {
    val btcdUrl: String = env("BTCD_URL", "http://cyber:cyber@127.0.0.1:8334")
}


object BitcoinPumpContext {

    val btcdClient: BitcoinJsonRpcClient = BitcoinJsonRpcClient(
            PumpsContext.jacksonJsonSerializer, PumpsContext.jacksonJsonDeserializer,
            PumpsContext.httpClient, BitcoinPumpConfiguration.btcdUrl
    )

    val bitcoinDaoService = BitcoinDaoService(PumpsContext.cassandra)

}