package fund.cyber.pump.bitcoin

import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinBlockTransaction
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.cassandra.*


class BitcoinCassandraActionFactory : CassandraStorageActionFactory<BitcoinBlockBundle> {

    override fun constructCassandraAction(bundle: BitcoinBlockBundle): CassandraStorageAction {
        return CompositeCassandraStorageAction(
                StoreValueCassandraStorageAction(bundle.block, BitcoinBlock::class.java),
                StoreListCassandraStorageAction(bundle.block.transactionPreviews, BitcoinBlockTransaction::class.java),
                StoreListCassandraStorageAction(bundle.transactions, BitcoinTransaction::class.java)
        )
    }
}