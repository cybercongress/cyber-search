package fund.cyber.pump.cassandra

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.node.common.Chain

object PumpsMigrations {

    const val pumpsApplicationIdPrefix = "PUMP."
    private const val applicationId = pumpsApplicationIdPrefix + "COMMON"

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/pump/0_initial.cql")
    )
}

fun chainApplicationId(chain: Chain): String {
    return PumpsMigrations.pumpsApplicationIdPrefix + chain
}
