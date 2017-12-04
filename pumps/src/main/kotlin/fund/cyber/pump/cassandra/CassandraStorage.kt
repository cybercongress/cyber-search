package fund.cyber.pump.cassandra

import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain.*
import fund.cyber.pump.*
import fund.cyber.pump.bitcoin.BitcoinBlockBundle
import fund.cyber.pump.bitcoin.BitcoinCassandraSaveAction
import fund.cyber.pump.bitcoin.BitcoinMigrations
import fund.cyber.pump.bitcoin.BitcoinPumpContext
import fund.cyber.pump.bitcoin_cash.BitcoinCashMigrations
import fund.cyber.pump.bitcoin_cash.BitcoinCashPumpContext
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock


class CassandraStorage: StorageInterface {
    override fun initialize(blockchains: List<Blockchain>) {
        val migrations = blockchains.flatMap(::getBlockchainInterfaceMigrations)
        PumpsContext.schemaMigrationEngine.executeSchemaUpdate(migrations)
    }


    override fun constructAction(blockBundle: BlockBundle): StorageAction {
        if (blockBundle is SimpleBlockBundle<*>) {
            return SimpleStorageAction(blockBundle)
        }

        return when (blockBundle.chain) {
            BITCOIN -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinPumpContext.bitcoinDaoService)
            BITCOIN_CASH -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinCashPumpContext.bitcoinDaoService)

            else -> StorageAction.empty
        }
    }
}


private fun getBlockchainInterfaceMigrations(blockchain: Blockchain): List<Migration> {
    if (blockchain is Migratory) {
        return blockchain.migrations
    }

    return when (blockchain.chain) {
        BITCOIN -> BitcoinMigrations.migrations
        BITCOIN_CASH -> BitcoinCashMigrations.migrations
        else -> emptyList()
    }
}
