package fund.cyber.pump.bitcoin

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.PumpsApplications.BITCOIN


object BitcoinMigrations {

    val migrations = listOf(
            CqlFileBasedMigration(0, BITCOIN, "/migrations/bitcoin/0_initial.cql"),
            ElasticHttpMigration(1, BITCOIN, "/migrations/bitcoin/1_create-tx-index.json"),
            ElasticHttpMigration(2, BITCOIN, "/migrations/bitcoin/2_create-tx-type.json"),
            ElasticHttpMigration(3, BITCOIN, "/migrations/bitcoin/3_create-block-index.json"),
            ElasticHttpMigration(4, BITCOIN, "/migrations/bitcoin/4_create-block-type.json"),
            ElasticHttpMigration(5, BITCOIN, "/migrations/bitcoin/5_create-address-index.json"),
            ElasticHttpMigration(6, BITCOIN, "/migrations/bitcoin/6_create-address-type.json")
    )
}