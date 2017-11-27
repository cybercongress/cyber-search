package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.awaitAll
import fund.cyber.pump.StorageAction


class BitcoinCassandraSaveAction(
        private val blockBundle: BitcoinBlockBundle,
        private val bitcoinDaoService: BitcoinDaoService
) : StorageAction {

    override fun store() = save()
    override fun remove() {}

    private fun save() {

        //save block and block tx preview
        val block = blockBundle.block
        block.transactionPreviews.map(bitcoinDaoService.blockTxStore::saveAsync).awaitAll()
        bitcoinDaoService.blockStore.save(block)

        //save tx
        blockBundle.transactions.map(bitcoinDaoService.txStore::saveAsync).awaitAll()
    }
}