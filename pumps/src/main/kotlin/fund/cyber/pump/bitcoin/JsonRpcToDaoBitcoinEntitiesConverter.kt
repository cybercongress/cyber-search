package fund.cyber.pump.bitcoin

import fund.cyber.dao.bitcoin.BitcoinDaoService
import fund.cyber.node.common.Chain
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.node.model.RegularTransactionInput


class JsonRpcBlockToBitcoinBundleConverter(
        private val chain: Chain,
        private val bitcoinDaoService: BitcoinDaoService,
        private val transactionConverter: JsonRpcToDaoBitcoinTransactionConverter,
        private val blockConverter: JsonRpcToDaoBitcoinBlockConverter
) {

    fun convertToBundle(jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinBlockBundle {

        val inputTransactions = getTransactionsInputs(jsonRpcBlock)
        val transactions = transactionConverter.convertToDaoTransactions(jsonRpcBlock, inputTransactions)
        val block = blockConverter.convertToDaoBlock(jsonRpcBlock, transactions)

        return BitcoinBlockBundle(
                hash = jsonRpcBlock.hash, parentHash = jsonRpcBlock.previousblockhash ?: "-1",
                number = jsonRpcBlock.height, block = block, transactions = transactions,
                chain = chain
        )
    }

    private fun getTransactionsInputs(jsonRpcBlock: JsonRpcBitcoinBlock): List<BitcoinTransaction> {

        val incomingNonCoinbaseTransactionsIds = jsonRpcBlock.rawtx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is RegularTransactionInput }
                .map { txInput -> (txInput as RegularTransactionInput).txid }

        return bitcoinDaoService.getTxs(incomingNonCoinbaseTransactionsIds)
    }
}

