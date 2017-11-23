package fund.cyber.pump.bitcoin

import fund.cyber.node.common.env
import fund.cyber.pump.PumpContext
import fund.cyber.pump.PumpConfiguration


class BitcoinPumpConfiguration : PumpConfiguration() {
    val btcdUrl: String = env("BTCD_URL", "http://cyber:cyber@127.0.0.1:8334")

}

fun main(args: Array<String>) {

    try {
        PumpContext.schemaMigrationEngine.executeSchemaUpdate(BitcoinMigrations.migrations)
    } finally {
        PumpContext.closeContext()
    }
}
