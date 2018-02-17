package fund.cyber.cassandra.migration

import fund.cyber.search.model.chains.Chain

val defaultSettings = MigrationSettings("default", "default")

open class MigrationSettings(
        val migrationDirectory: String,
        val applicationId: String
)

open class BlockchainMigrationSettings(
        chain: Chain
) : MigrationSettings(
        migrationDirectory = chain.lowerCaseName,
        applicationId = chain.lowerCaseName
)