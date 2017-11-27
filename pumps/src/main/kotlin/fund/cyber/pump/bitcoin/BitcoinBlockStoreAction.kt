package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.awaitAll
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.pump.StorageAction


class BitcoinBlockSaveAction(
        val block: BitcoinBlock,
        private val bitcoinDaoService: BitcoinDaoService,
        private val transactionConverter: JsonRpcToDaoBitcoinTransactionConverter,
        private val blockConverter: JsonRpcToDaoBitcoinBlockConverter
) : StorageAction {

    override fun store() = save()
    override fun remove() {}

    private fun save() {

        /* val inputTransactions = getTransactionsInputs()
         val transactions = transactionConverter.convertToDaoTransactions(jsonRpcBlock, inputTransactions)
         val block = blockConverter.convertToDaoBlock(jsonRpcBlock, transactions)
 */
        block.transactionPreviews.map(bitcoinDaoService.blockTxStore::saveAsync).awaitAll()
        bitcoinDaoService.blockStore.save(block)
    }

/*    private fun getTransactionsInputs(): List<BitcoinTransaction> {

        val incomingNonCoinbaseTransactionsIds = jsonRpcBlock.rawtx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is RegularTransactionInput }
                .map { txInput -> (txInput as RegularTransactionInput).txid }

        return bitcoinDaoService.getTxs(incomingNonCoinbaseTransactionsIds)
    }*/
}