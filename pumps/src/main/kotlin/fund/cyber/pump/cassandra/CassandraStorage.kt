package fund.cyber.pump.cassandra

import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.pump.*
import fund.cyber.pump.bitcoin.BitcoinBlockBundle
import fund.cyber.pump.bitcoin.BitcoinCassandraSaveAction
import fund.cyber.pump.bitcoin.BitcoinMigrations
import fund.cyber.pump.bitcoin.BitcoinPumpContext
import fund.cyber.pump.bitcoin_cash.BitcoinCashMigrations
import fund.cyber.pump.bitcoin_cash.BitcoinCashPumpContext
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock


class CassandraStorage : StorageInterface {

    override fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>) {
        val migrations = blockchainInterfaces.flatMap(::getBlockchainInterfaceMigrations)
        PumpsContext.schemaMigrationEngine.executeSchemaUpdate(migrations)
    }


    override fun constructAction(blockBundle: BlockBundle): StorageAction {
        return when (blockBundle.chain) {
            BITCOIN -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinPumpContext.bitcoinDaoService)
            BITCOIN_CASH -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinCashPumpContext.bitcoinDaoService)
            else -> StorageAction.empty
        }
    }
}


private fun getBlockchainInterfaceMigrations(blockchainInterface: BlockchainInterface<*>): List<Migration> {
    return when (blockchainInterface.chain) {
        BITCOIN -> BitcoinMigrations.migrations
        BITCOIN_CASH -> BitcoinCashMigrations.migrations
        else -> emptyList()
    }
}