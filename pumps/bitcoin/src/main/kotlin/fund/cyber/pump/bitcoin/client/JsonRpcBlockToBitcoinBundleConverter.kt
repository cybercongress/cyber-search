package fund.cyber.pump.bitcoin.client

import cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import cyber.search.model.bitcoin.RegularTransactionInput
import org.ehcache.Cache
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


private val log = LoggerFactory.getLogger(JsonRpcBlockToBitcoinBundleConverter::class.java)!!

//todo add cache
@Component
class JsonRpcBlockToBitcoinBundleConverter(
        private val client: BitcoinJsonRpcClient,
        private val txCache: Cache<String, JsonRpcBitcoinTransaction>? = null
) {

    private val transactionConverter = JsonRpcToDaoBitcoinTxConverter()
    private val blockConverter = JsonRpcToDaoBitcoinBlockConverter()


    fun convertToBundle(jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinBlockBundle {

        jsonRpcBlock.rawtx.forEach { tx -> txCache?.put(tx.txid, tx) }

        val inputTransactions = getTransactionsInputs(jsonRpcBlock)
        val transactions = transactionConverter.convertToDaoTransactions(jsonRpcBlock, inputTransactions)
        val block = blockConverter.convertToDaoBlock(jsonRpcBlock, transactions)

        return BitcoinBlockBundle(
                hash = jsonRpcBlock.hash, parentHash = jsonRpcBlock.previousblockhash ?: "-1",
                number = jsonRpcBlock.height, block = block, transactions = transactions
        )
    }


    private fun getTransactionsInputs(jsonRpcBlock: JsonRpcBitcoinBlock): List<JsonRpcBitcoinTransaction> {

        val incomingNonCoinbaseTransactionsIds = jsonRpcBlock.rawtx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is RegularTransactionInput }
                .map { txInput -> (txInput as RegularTransactionInput).txid }

        if (incomingNonCoinbaseTransactionsIds.isEmpty()) return emptyList()

        if (txCache != null) {

            val txs = mutableListOf<JsonRpcBitcoinTransaction>()
            val idsWithoutCacheHit = mutableListOf<String>()

            for (id in incomingNonCoinbaseTransactionsIds) {
                val tx = txCache[id]
                if (tx != null) txs.add(tx) else idsWithoutCacheHit.add(id)
            }

            log.debug("Transactions - Total ids: ${incomingNonCoinbaseTransactionsIds.size}, Cache hits: ${txs.size}")

            txs.addAll(client.getTxes(idsWithoutCacheHit))
            return txs
        }

        return client.getTxes(incomingNonCoinbaseTransactionsIds)
    }
}

