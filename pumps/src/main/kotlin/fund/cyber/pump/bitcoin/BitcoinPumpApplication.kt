package fund.cyber.pump.bitcoin

import fund.cyber.node.common.env
import fund.cyber.pump.PumpContext
import fund.cyber.pump.PumpConfiguration
import fund.cyber.pump.PumpsApplications.BITCOIN
import getStartBlockNumber
import org.slf4j.LoggerFactory


class BitcoinPumpConfiguration : PumpConfiguration() {
    val btcdUrl: String = env("BTCD_URL", "http://cyber:cyber@127.0.0.1:8334")
}


private val log = LoggerFactory.getLogger(BitcoinPumpConfiguration::class.java)!!

fun main(args: Array<String>) {

    try {
        PumpContext.schemaMigrationEngine.executeSchemaUpdate(BitcoinMigrations.migrations)
        val startBlockNumber = getStartBlockNumber(BITCOIN, PumpContext.pumpDaoService, PumpContext.configuration)
        log.info("Bitcoin application started from block $startBlockNumber")
    } finally {
        PumpContext.closeContext()
    }
}