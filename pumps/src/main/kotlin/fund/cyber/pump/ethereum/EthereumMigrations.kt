package fund.cyber.pump.applicationId

import Chains.ETHEREUM
import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.PumpsMigrations

object EthereumMigrations {

    val applicationId = PumpsMigrations.pumpsApplicationIdPrefix + ETHEREUM

    val migrations = listOf(
            CqlFileBasedMigration(0, applicationId, "/migrations/applicationId/0_initial.cql"),
            ElasticHttpMigration(1, applicationId, "/migrations/applicationId/1_create-tx-index.json"),
            ElasticHttpMigration(2, applicationId, "/migrations/applicationId/2_create-tx-type.json"),
            ElasticHttpMigration(3, applicationId, "/migrations/applicationId/3_create-block-index.json"),
            ElasticHttpMigration(4, applicationId, "/migrations/applicationId/4_create-block-type.json"),
            ElasticHttpMigration(5, applicationId, "/migrations/applicationId/5_create-address-index.json"),
            ElasticHttpMigration(6, applicationId, "/migrations/applicationId/6_create-address-type.json")
    )
}