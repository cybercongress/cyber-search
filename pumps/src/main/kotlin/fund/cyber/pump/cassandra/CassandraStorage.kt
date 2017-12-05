package fund.cyber.pump.cassandra

import com.datastax.driver.mapping.MappingManager
import fund.cyber.dao.migration.Migratable
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.common.Chain.BITCOIN_CASH
import fund.cyber.node.model.CyberSearchItem
import fund.cyber.pump.*
import fund.cyber.pump.bitcoin.BitcoinBlockBundle
import fund.cyber.pump.bitcoin.BitcoinCassandraSaveAction
import fund.cyber.pump.bitcoin.BitcoinPumpContext
import fund.cyber.pump.bitcoin_cash.BitcoinCashPumpContext
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock


class CassandraStorage : StorageInterface {

    override fun initialize(blockchains: List<Blockchain>) {

        val migrations = blockchains.filterIsInstance(Migratable::class.java).flatMap(Migratable::migrations)
        PumpsContext.schemaMigrationEngine.executeSchemaUpdate(migrations)

        blockchains.forEach { blockchain ->
            SimpleBlockBundle.actionSourceFactories += CassandraActionSourceFactory(blockchain.chain)
        }
    }


    override fun constructAction(blockBundle: BlockBundle): StorageAction {
        if (blockBundle is SimpleBlockBundle) {
            return SimpleStorageAction(blockBundle)
        }

        return when (blockBundle.chain) {
            BITCOIN -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinPumpContext.bitcoinDaoService)
            BITCOIN_CASH -> BitcoinCassandraSaveAction(blockBundle as BitcoinBlockBundle, BitcoinCashPumpContext.bitcoinDaoService)

            else -> StorageAction.empty
        }
    }
}

class CassandraActionSourceFactory(override val chain: Chain) : ActionSourceFactory {
    val session = PumpsContext.cassandra.connect(chain.name.toLowerCase())
    val manager = MappingManager(session)

    override fun <R : CyberSearchItem> actionFor(value: R, cls: Class<R>): Pair<() -> Unit, () -> Unit> {
        val mapper = manager.mapper(cls)
        mapper.save(value)
        return Pair(
                { mapper.save(value) },
                { mapper.delete(value) }
        )
    }
}