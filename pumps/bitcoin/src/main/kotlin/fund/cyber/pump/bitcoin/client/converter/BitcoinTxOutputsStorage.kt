package fund.cyber.pump.bitcoin.client.converter

import fund.cyber.pump.bitcoin.client.BitcoinJsonRpcClient
import fund.cyber.search.configuration.BTC_TX_DOWNLOAD_MAX_CONCURRENCY
import fund.cyber.search.configuration.BTC_TX_DOWNLOAD_MAX_CONCURRENCY_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import fund.cyber.search.model.bitcoin.RegularTransactionInput
import io.micrometer.core.instrument.MeterRegistry
import org.ehcache.Cache
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicLong

private val log = LoggerFactory.getLogger(BitcoinTxOutputsStorage::class.java)!!

@Component
class BitcoinTxOutputsStorage(
    private val client: BitcoinJsonRpcClient,
    private val cache: Cache<String, BitcoinCacheTxOutput>? = null,
    monitoring: MeterRegistry,
    @Value("\${$BTC_TX_DOWNLOAD_MAX_CONCURRENCY:$BTC_TX_DOWNLOAD_MAX_CONCURRENCY_DEFAULT}")
    private val txMaxConcurrency: String
) {

    val linkedOutputsFromCache = monitoring.gauge("linked_outputs_from_cache", AtomicLong(0L))!!
    val totalLinkedOutputs = monitoring.gauge("total_linked_outputs", AtomicLong(0L))!!

    fun updateCache(jsonRpcBlock: JsonRpcBitcoinBlock) {
        jsonRpcBlock.tx.forEach { tx ->
            tx.vout.forEach { out ->
                cache?.put((tx.txid to out.n).txOutputCacheKey(), BitcoinCacheTxOutput(tx.txid, out))
            }
        }
    }

    fun getLinkedOutputsByBlock(jsonRpcBlock: JsonRpcBitcoinBlock): List<BitcoinCacheTxOutput> {

        val incomingNonCoinbaseTxsLinkedOutputsIds = jsonRpcBlock.tx
            .flatMap { transaction -> transaction.vin }
            .filter { txInput -> txInput is RegularTransactionInput }
            .map { txInput -> (txInput as RegularTransactionInput).txid to txInput.vout }

        if (incomingNonCoinbaseTxsLinkedOutputsIds.isEmpty()) return emptyList()

        return getLinkedOutputs(incomingNonCoinbaseTxsLinkedOutputsIds)
    }

    fun getLinkedOutputsByTx(jsonRpcTx: JsonRpcBitcoinTransaction): List<BitcoinCacheTxOutput> {
        val linkedOutputsIds = jsonRpcTx.vin
            .filter { txInput -> txInput is RegularTransactionInput }
            .map { txInput -> (txInput as RegularTransactionInput).txid to txInput.vout }

        return getLinkedOutputs(linkedOutputsIds)
    }

    private fun getLinkedOutputs(linkedOutputs: List<Pair<String, Int>>): List<BitcoinCacheTxOutput> {
        if (cache != null) {

            val outputsFromCache = mutableListOf<BitcoinCacheTxOutput>()
            val outputsIdsWithoutCacheHit = mutableListOf<Pair<String, Int>>()

            for (txOutputId in linkedOutputs) {
                val output = cache[txOutputId.txOutputCacheKey()]
                if (output != null) {
                    outputsFromCache.add(output)
                    cache.remove(txOutputId.txOutputCacheKey())
                } else outputsIdsWithoutCacheHit.add(txOutputId)
            }

            log.debug("Transactions outputs - Total ids: ${linkedOutputs.size}," +
                " Cache hits: ${outputsFromCache.size}")
            totalLinkedOutputs.set(linkedOutputs.size.toLong())
            linkedOutputsFromCache.set(outputsFromCache.size.toLong())

            val nonCacheOutputs = client
                .getTxes(outputsIdsWithoutCacheHit.map { out -> out.first }.toSet(), txMaxConcurrency.toInt())
                .flatMap { tx -> tx.vout.map { out -> BitcoinCacheTxOutput(tx.txid, out) } }
            return outputsFromCache + nonCacheOutputs
        }

        return client
            .getTxes(linkedOutputs.map { out -> out.first }.toSet(), txMaxConcurrency.toInt())
            .flatMap { tx -> tx.vout.map { out -> BitcoinCacheTxOutput(tx.txid, out) } }
    }


}
