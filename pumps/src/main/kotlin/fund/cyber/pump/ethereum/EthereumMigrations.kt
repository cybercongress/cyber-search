package fund.cyber.pump.ethereum

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.PumpsApplications.ETHEREUM

object EthereumMigrations {

    val migrations = listOf(
            CqlFileBasedMigration(0, ETHEREUM, "/migrations/ethereum/0_initial.cql"),
            ElasticHttpMigration(1, ETHEREUM, "/migrations/ethereum/1_create-tx-index.json"),
            ElasticHttpMigration(2, ETHEREUM, "/migrations/ethereum/2_create-tx-type.json"),
            ElasticHttpMigration(3, ETHEREUM, "/migrations/ethereum/3_create-block-index.json"),
            ElasticHttpMigration(4, ETHEREUM, "/migrations/ethereum/4_create-block-type.json"),
            ElasticHttpMigration(5, ETHEREUM, "/migrations/ethereum/5_create-address-index.json"),
            ElasticHttpMigration(6, ETHEREUM, "/migrations/ethereum/6_create-address-type.json")
    )
}