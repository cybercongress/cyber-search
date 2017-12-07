package fund.cyber.pump.ethereum

import fund.cyber.node.common.Chain.*
import fund.cyber.cassandra.migration.CqlFileBasedMigration
import fund.cyber.cassandra.migration.ElasticHttpMigration
import fund.cyber.pump.cassandra.chainApplicationId

object EthereumMigrations {

    private val applicationId = chainApplicationId(ETHEREUM)

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/ethereum/0_initial.cql"),
            ElasticHttpMigration(1, applicationId, "/migrations/ethereum/1_create-tx-index.json"),
            ElasticHttpMigration(2, applicationId, "/migrations/ethereum/2_create-tx-type.json"),
            ElasticHttpMigration(3, applicationId, "/migrations/ethereum/3_create-block-index.json"),
            ElasticHttpMigration(4, applicationId, "/migrations/ethereum/4_create-block-type.json"),
            ElasticHttpMigration(5, applicationId, "/migrations/ethereum/5_create-address-index.json"),
            ElasticHttpMigration(6, applicationId, "/migrations/ethereum/6_create-address-type.json")
    )
}