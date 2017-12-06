package fund.cyber.pump.ethereum

import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.pump.cassandra.*

class EthereumCassandraActionFactory : CassandraStorageActionFactory<EthereumBlockBundle> {

    override fun constructCassandraAction(bundle: EthereumBlockBundle): CassandraStorageAction {
        return CompositeCassandraStorageAction(
                StoreValueCassandraStorageAction(bundle.block, EthereumBlock::class.java),
                StoreListCassandraStorageAction(bundle.transactions, EthereumTransaction::class.java)
        )
    }
}