package fund.cyber.pump.cassandra

import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.pump.*
import fund.cyber.pump.bitcoin.BitcoinBlockBundle
import fund.cyber.pump.bitcoin.BitcoinCassandraSaveAction
import fund.cyber.pump.bitcoin.BitcoinMigrations
import fund.cyber.pump.bitcoin.BitcoinPumpContext
import fund.cyber.pump.bitcoin_cash.BitcoinCashMigrations
import fund.cyber.pump.bitcoin_cash.BitcoinCashPumpContext
import fund.cyber.pump.ethereum.EthereumBlockBundle
import fund.cyber.pump.ethereum.EthereumCassandraSaveAction
import fund.cyber.pump.ethereum.EthereumMigrations
import fund.cyber.pump.ethereum_classic.EthereumClassicMigrations
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock


class CassandraStorage : StorageInterface {
    private val ethereumDao by lazy { EthereumDaoService(PumpsContext.cassandra) }
    private val ethereumClassicDao by lazy { EthereumDaoService(PumpsContext.cassandra, Chain.ETHEREUM_CLASSIC.name) }

    override fun initialize(blockchainInterfaces: List<BlockchainInterface<*>>) {
        val migrations = blockchainInterfaces.flatMap(::getBlockchainInterfaceMigrations)
        PumpsContext.schemaMigrationEngine.executeSchemaUpdate(migrations)
    }


    override fun constructAction(blockBundle: BlockBundle): StorageAction {
        return when (blockBundle.chain) {
            BITCOIN -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinPumpContext.bitcoinDaoService)
            BITCOIN_CASH -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinCashPumpContext.bitcoinDaoService)

            ETHEREUM -> EthereumCassandraSaveAction(blockBundle as EthereumBlockBundle, ethereumDao)
            ETHEREUM_CLASSIC -> EthereumCassandraSaveAction(blockBundle as EthereumBlockBundle, ethereumClassicDao)

            else -> StorageAction.empty
        }
    }
}


private fun getBlockchainInterfaceMigrations(blockchainInterface: BlockchainInterface<*>): List<Migration> {
    return when (blockchainInterface.chain) {
        BITCOIN -> BitcoinMigrations.migrations
        BITCOIN_CASH -> BitcoinCashMigrations.migrations
        ETHEREUM -> EthereumMigrations.migrations
        ETHEREUM_CLASSIC -> EthereumClassicMigrations.migrations
        else -> emptyList()
    }
}
