package fund.cyber.pump.bitcoin

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration


object BitcoinMigrations {

    val migrations = listOf(
            CqlFileBasedMigration(0, "pump.bitcoin", "/migrations/bitcoin/0_initial.cql"),
            ElasticHttpMigration(1, "pump.bitcoin", "/migrations/bitcoin/1_create-tx-index.json"),
            ElasticHttpMigration(2, "pump.bitcoin", "/migrations/bitcoin/2_create-tx-type.json"),
            ElasticHttpMigration(3, "pump.bitcoin", "/migrations/bitcoin/3_create-block-index.json"),
            ElasticHttpMigration(4, "pump.bitcoin", "/migrations/bitcoin/4_create-block-type.json"),
            ElasticHttpMigration(5, "pump.bitcoin", "/migrations/bitcoin/5_create-address-index.json"),
            ElasticHttpMigration(6, "pump.bitcoin", "/migrations/bitcoin/6_create-address-type.json")
    )
}