package fund.cyber.pump

import fund.cyber.dao.migration.CqlFileBasedMigration

object PumpsMigrations {

    const val pumpsApplicationIdPrefix = "PUMP."
    private const val applicationId = pumpsApplicationIdPrefix + "COMMON"

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/pump/0_initial.cql")
    )
}