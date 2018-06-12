package fund.cyber.cassandra.migration

import fund.cyber.search.model.chains.ChainInfo

val defaultSettings = MigrationSettings("default", "default")

open class MigrationSettings(
    val migrationDirectory: String,
    val applicationId: String
)

open class BlockchainMigrationSettings(
    chainInfo: ChainInfo
) : MigrationSettings(
    migrationDirectory = chainInfo.familyNameLowerCase,
    applicationId = chainInfo.nameLowerCase
)
