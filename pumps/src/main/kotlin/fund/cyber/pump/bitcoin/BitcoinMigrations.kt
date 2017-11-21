package fund.cyber.pump.bitcoin

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration


object BitcoinMigrations {

    val migrations = listOf(
            CqlFileBasedMigration(0, "pump.bitcoin", "/migrations/bitcoin/0_initial.cql"),
            ElasticHttpMigration(1, "pump.bitcoin", "/migrations/bitcoin/1_create-tx-index.json"),
            ElasticHttpMigration(2, "pump.bitcoin", "/migrations/bitcoin/2_create-tx-type.json")
    )
}