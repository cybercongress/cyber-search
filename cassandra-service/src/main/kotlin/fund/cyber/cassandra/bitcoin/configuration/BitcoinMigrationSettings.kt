package fund.cyber.cassandra.bitcoin.configuration

import fund.cyber.cassandra.migration.MigrationSettings
import fund.cyber.search.model.chains.BitcoinFamilyChain

open class BitcoinMigrationSettings(
        chain: BitcoinFamilyChain
) : MigrationSettings(
        migrationDirectory = chain.name.toLowerCase(),
        applicationId = chain.name.toLowerCase()
)