package fund.cyber.pump.ethereum_classic

import fund.cyber.node.common.Chain.*
import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.cassandra.chainApplicationId


object EthereumClassicMigrations {

    private val applicationId = chainApplicationId(ETHEREUM_CLASSIC)


    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/ethereum_classic/0_initial.cql"),
            ElasticHttpMigration(1, applicationId, "/migrations/ethereum_classic/1_create-tx-index.json"),
            ElasticHttpMigration(2, applicationId, "/migrations/ethereum_classic/2_create-tx-type.json"),
            ElasticHttpMigration(3, applicationId, "/migrations/ethereum_classic/3_create-block-index.json"),
            ElasticHttpMigration(4, applicationId, "/migrations/ethereum_classic/4_create-block-type.json"),
            ElasticHttpMigration(5, applicationId, "/migrations/ethereum_classic/5_create-address-index.json"),
            ElasticHttpMigration(6, applicationId, "/migrations/ethereum_classic/6_create-address-type.json")
    )
}