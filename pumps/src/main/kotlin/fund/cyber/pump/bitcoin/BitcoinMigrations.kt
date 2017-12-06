package fund.cyber.pump.bitcoin

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.node.common.Chain.*
import fund.cyber.pump.chainApplicationId


object BitcoinMigrations {

    private val applicationId = chainApplicationId(BITCOIN)

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/bitcoin/0_initial.cql"),
            ElasticHttpMigration(1, applicationId, "/migrations/bitcoin/1_create-tx-index.json"),
            ElasticHttpMigration(2, applicationId, "/migrations/bitcoin/2_create-tx-type.json"),
            ElasticHttpMigration(3, applicationId, "/migrations/bitcoin/3_create-block-index.json"),
            ElasticHttpMigration(4, applicationId, "/migrations/bitcoin/4_create-block-type.json"),
            ElasticHttpMigration(5, applicationId, "/migrations/bitcoin/5_create-address-index.json"),
            ElasticHttpMigration(6, applicationId, "/migrations/bitcoin/6_create-address-type.json"),
            CqlFileBasedMigration(7, applicationId, "/migrations/bitcoin/7_genesis.cql")
    )
}