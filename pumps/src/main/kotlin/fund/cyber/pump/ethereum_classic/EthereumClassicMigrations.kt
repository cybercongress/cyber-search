package fund.cyber.pump.ethereum_classic

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.PumpsApplications.ETHEREUM_CLASSIC


object EthereumClassicMigrations {


    val migrations = listOf(
            CqlFileBasedMigration(0, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/0_initial.cql"),
            ElasticHttpMigration(1, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/1_create-tx-index.json"),
            ElasticHttpMigration(2, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/2_create-tx-type.json"),
            ElasticHttpMigration(3, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/3_create-block-index.json"),
            ElasticHttpMigration(4, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/4_create-block-type.json"),
            ElasticHttpMigration(5, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/5_create-address-index.json"),
            ElasticHttpMigration(6, ETHEREUM_CLASSIC, "/migrations/ethereum_classic/6_create-address-type.json")
    )
}