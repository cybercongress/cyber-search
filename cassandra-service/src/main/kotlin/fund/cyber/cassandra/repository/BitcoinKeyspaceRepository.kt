package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import com.google.common.util.concurrent.ListenableFuture
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.model.*
import org.ehcache.Cache
import java.util.*

class BitcoinKeyspaceRepository(
        cassandra: Cluster, chain: Chain
) : CassandraKeyspaceRepository(cassandra, chain.lowercaseName()) {


    private val txCache: Cache<String, BitcoinTransaction>? = null
    private val addressCache: Cache<String, BitcoinAddress>? = null

    val blockStore = mappingManager.mapper(BitcoinBlock::class.java)!!
    val blockTxStore = mappingManager.mapper(BitcoinBlockTransaction::class.java)!!
    val txStore = mappingManager.mapper(BitcoinTransaction::class.java)!!
    val addressStore = mappingManager.mapper(BitcoinAddress::class.java)!!
    val addressTxtore = mappingManager.mapper(BitcoinAddressTransaction::class.java)!!


    fun saveAsyncTx(tx: BitcoinTransaction): ListenableFuture<Void> {
        txCache?.put(tx.hash, tx)
        return txStore.saveAsync(tx)
    }


    fun getMempoolTxesHashes(): List<String> = session
            .execute("SELECT hash FROM tx_preview_by_block WHERE block_number=$TX_MEMORY_POOL_BLOCK_NUMBER")
            .map { row -> row.getString("hash") }


    fun getTxs(ids: List<String>): List<BitcoinTransaction> {

        if (ids.isEmpty()) return emptyList()

        if (txCache != null) {

            val txs = mutableListOf<BitcoinTransaction>()
            val idsWithoutCacheHit = mutableListOf<String>()

            for (id in ids) {
                val tx = txCache[id]
                if (tx != null) txs.add(tx) else idsWithoutCacheHit.add(id)
            }


            txs.addAll(queryTxsByIds(idsWithoutCacheHit))
            return txs
        }

        return queryTxsByIds(ids)
    }

    private fun queryTxsByIds(ids: List<String>): List<BitcoinTransaction> {

        val statement = session.prepare("SELECT * FROM tx WHERE hash = ?")

        return ids.map { id -> session.executeAsync(statement.bind(id)) } // future<ResultSet>
                .map { futureResultSet ->
                    // cql row
                    while (!futureResultSet.isDone) Thread.sleep(10)
                    futureResultSet.get().one()
                }
                .filter(Objects::nonNull)
                .map(this::bitcoinTransactionMapping)
    }


    private fun bitcoinTransactionMapping(row: Row): BitcoinTransaction {
        return BitcoinTransaction(
                hash = row.getString("hash"), fee = row.getString("fee"), size = row.getInt("size"),
                block_number = row.getLong("block_number"),
                total_output = row.getString("total_output"), total_input = row.getString("total_input"),
                block_time = row.getTimestamp("block_time").toInstant(),
                coinbase = row.getString("coinbase"), block_hash = row.getString("block_hash"),
                ins = row.getList("ins", BitcoinTransactionIn::class.java),
                outs = row.getList("outs", BitcoinTransactionOut::class.java)
        )
    }


    private fun bitcoinBlockMapping(row: Row): BitcoinBlock {
        return BitcoinBlock(
                height = row.getLong("height"), hash = row.getString("hash"), size = row.getInt("size"),
                time = row.getTimestamp("time").toInstant(), nonce = row.getLong("nonce"),
                merkleroot = row.getString("merkleroot"), version = row.getInt("version"),
                weight = row.getInt("weight"), bits = row.getString("bits"), tx_number = row.getInt("tx_number"),
                total_outputs_value = row.getString("total_outputs_value"), difficulty = row.getVarint("difficulty")
        )
    }
}