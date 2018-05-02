package fund.cyber.pump.bitcoin.client

import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.RegularTransactionInput
import io.micrometer.core.instrument.MeterRegistry
import org.ehcache.Cache
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong


private val log = LoggerFactory.getLogger(JsonRpcBlockToBitcoinBundleConverter::class.java)!!

fun Pair<String, Int>.txOutputCacheKey(): String = "${first}__$second"

//todo add cache
@Component
class JsonRpcBlockToBitcoinBundleConverter(
    private val client: BitcoinJsonRpcClient,
    private val txOutputCache: Cache<String, BitcoinCacheTxOutput>? = null,
    monitoring: MeterRegistry
) {

    val linkedOutputsFromCache = monitoring.gauge("linked_outputs_from_cache", AtomicLong(0L))!!
    val totalLinkedOutputs = monitoring.gauge("total_linked_outputs", AtomicLong(0L))!!

    private val transactionConverter = JsonRpcToDaoBitcoinTxConverter()
    private val blockConverter = JsonRpcToDaoBitcoinBlockConverter()


    fun convertToBundle(jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinBlockBundle {

        updateTxOutputCache(jsonRpcBlock)

        val linkedOutputs = getLinkedOutputs(jsonRpcBlock)
        val transactions = transactionConverter.convertToDaoTransactions(jsonRpcBlock, linkedOutputs)
        val block = blockConverter.convertToDaoBlock(jsonRpcBlock, transactions)

        return BitcoinBlockBundle(
                hash = jsonRpcBlock.hash, parentHash = jsonRpcBlock.previousblockhash ?: "-1",
                number = jsonRpcBlock.height, block = block, transactions = transactions,
                blockSize = jsonRpcBlock.size
        )
    }

    private fun updateTxOutputCache(jsonRpcBlock: JsonRpcBitcoinBlock) {
        jsonRpcBlock.tx.forEach { tx ->
            tx.vout.forEach { out ->
                txOutputCache?.put((tx.txid to out.n).txOutputCacheKey(), BitcoinCacheTxOutput(tx.txid, out))
            }
        }
    }


    private fun getLinkedOutputs(jsonRpcBlock: JsonRpcBitcoinBlock): List<BitcoinCacheTxOutput> {

        val incomingNonCoinbaseTxsLinkedOutputsIds = jsonRpcBlock.tx
                .flatMap { transaction -> transaction.vin }
                .filter { txInput -> txInput is RegularTransactionInput }
                .map { txInput -> (txInput as RegularTransactionInput).txid to txInput.vout }

        if (incomingNonCoinbaseTxsLinkedOutputsIds.isEmpty()) return emptyList()

        if (txOutputCache != null) {

            val outputsFromCache = mutableListOf<BitcoinCacheTxOutput>()
            val outputsIdsWithoutCacheHit = mutableListOf<Pair<String, Int>>()

            for (txOutputId in incomingNonCoinbaseTxsLinkedOutputsIds) {
                val output = txOutputCache[txOutputId.txOutputCacheKey()]
                if (output != null) {
                    outputsFromCache.add(output)
                    txOutputCache.remove(txOutputId.txOutputCacheKey())
                } else outputsIdsWithoutCacheHit.add(txOutputId)
            }

            log.debug("Transactions outputs - Total ids: ${incomingNonCoinbaseTxsLinkedOutputsIds.size}," +
                " Cache hits: ${outputsFromCache.size}")
            totalLinkedOutputs.set(incomingNonCoinbaseTxsLinkedOutputsIds.size.toLong())
            linkedOutputsFromCache.set(outputsFromCache.size.toLong())

            val nonCacheOutputs = client.getTxes(outputsIdsWithoutCacheHit.map { out -> out.first }.toSet())
                .flatMap { tx -> tx.vout.map { out -> BitcoinCacheTxOutput(tx.txid, out) } }
            return outputsFromCache + nonCacheOutputs
        }

        return client
            .getTxes(incomingNonCoinbaseTxsLinkedOutputsIds.map { out -> out.first }.toSet())
            .flatMap { tx -> tx.vout.map { out -> BitcoinCacheTxOutput(tx.txid, out) } }
    }
}
