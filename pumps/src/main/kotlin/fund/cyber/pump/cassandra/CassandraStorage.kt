package fund.cyber.pump.cassandra

import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.PumpsContext
import fund.cyber.pump.StorageInterface
import fund.cyber.pump.bitcoin.BitcoinMigrations
import fund.cyber.pump.bitcoin_cash.BitcoinCashMigrations
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock


class CassandraStorage : StorageInterface {

    override fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>) {

        val migrations = blockchainInterfaces.flatMap(::getBlockchainInterfaceMigrations)
        PumpsContext.schemaMigrationEngine.executeSchemaUpdate(migrations)
    }
}

private fun getBlockchainInterfaceMigrations(blockchainInterface: BlockchainInterface<*>): List<Migration> {
    return when (blockchainInterface.chain) {
        BITCOIN -> BitcoinMigrations.migrations
        BITCOIN_CASH -> BitcoinCashMigrations.migrations
        else -> emptyList()
    }
}