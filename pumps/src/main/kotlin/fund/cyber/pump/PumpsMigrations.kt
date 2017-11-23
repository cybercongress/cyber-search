package fund.cyber.pump

import fund.cyber.dao.migration.CqlFileBasedMigration

object PumpsMigrations {

    private val applicationId = "pump.common"

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/pump/0_initial.cql")
    )
}