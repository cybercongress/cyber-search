package fund.cyber.dao.bitcoin

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.common.Chain
import fund.cyber.node.model.*
import org.ehcache.Cache
import org.slf4j.LoggerFactory
import java.util.*


private val log = LoggerFactory.getLogger(BitcoinDaoService::class.java)!!

class BitcoinDaoService(
        cassandra: Cluster,
        chain: Chain = Chain.BITCOIN,
        private val txCache: Cache<String, BitcoinTransaction>? = null,
        private val addressCache: Cache<String, BitcoinAddress>? = null
) {

    private val session: Session = cassandra.connect(chain.name)
    private val mappingManager = MappingManager(session)

    val blockStore = mappingManager.mapper(BitcoinBlock::class.java)!!
    val blockTxStore = mappingManager.mapper(BitcoinBlockTransaction::class.java)!!
    val txStore = mappingManager.mapper(BitcoinTransaction::class.java)!!
    val addressStore = mappingManager.mapper(BitcoinAddress::class.java)!!
    val addressTxtore = mappingManager.mapper(BitcoinAddressTransaction::class.java)!!


    fun getMempoolTxesHashes(): List<String> = session
            .execute("SELECT hash FROM tx_preview_by_block WHERE block_number=$TX_MEMORY_POOL_BLOCK_NUMBER")
            .map { row -> row.getString("hash") }


    fun getAddress(id: String): BitcoinAddress? = session
            .execute("SELECT * FROM address WHERE id='$id'")
            .map(this::bitcoinAddressMapping).firstOrNull()


    fun getBlockByNumber(number: Long): BitcoinBlock? = session
            .execute("SELECT * FROM block WHERE height=$number")
            .map(this::bitcoinBlockMapping).firstOrNull()


    fun getTxById(id: String): BitcoinTransaction? = session
            .execute("SELECT * FROM tx WHERE txid='$id'")
            .map(this::bitcoinTransactionMapping).firstOrNull()


    fun getTxs(ids: List<String>): List<BitcoinTransaction> {

        if (ids.isEmpty()) return emptyList()

        if (txCache != null) {

            val txs = mutableListOf<BitcoinTransaction>()
            val idsWithoutCacheHit = mutableListOf<String>()

            for (id in ids) {
                val tx = txCache[id]
                if (tx != null) txs.add(tx) else idsWithoutCacheHit.add(id)
            }

            log.debug("Transactions - Total ids: ${ids.size}, Cache hits: ${txs.size}")

            txs.addAll(queryTxsByIds(idsWithoutCacheHit))
            return txs
        }

        return queryTxsByIds(ids)
    }


    fun getAddressesWithLastTransactionBeforeGivenBlock(ids: List<String>, blockNumber: Long): List<BitcoinAddress> {

        if (ids.isEmpty()) return emptyList()

        return when (addressCache) {
            null -> queryAddressesWithLastTransactionBeforeGivenBlock(ids, blockNumber)
            else -> {
                val addresses = mutableListOf<BitcoinAddress>()
                val idsWithoutCacheHit = mutableListOf<String>()

                for (id in ids) {

                    val address = addressCache[id]
                    if (address != null && address.last_transaction_block < blockNumber)
                        addresses.add(address)
                    else
                        idsWithoutCacheHit.add(id)
                }

                log.debug("Address - Total ids: ${ids.size}, Cache hits: ${addresses.size}")

                addresses.addAll(queryAddressesWithLastTransactionBeforeGivenBlock(idsWithoutCacheHit, blockNumber))
                return addresses
            }
        }
    }

    private fun queryAddressesWithLastTransactionBeforeGivenBlock(ids: List<String>, blockNumber: Long): List<BitcoinAddress> {

        val statement = session.prepare("SELECT * FROM address WHERE id=? AND last_transaction_block < $blockNumber ALLOW FILTERING")

        return ids.map { id -> session.executeAsync(statement.bind(id)) } // future<ResultSet>
                .map { futureResultSet ->
                    // cql row
                    while (!futureResultSet.isDone) Thread.sleep(10)
                    futureResultSet.get().one()
                }
                .filter(Objects::nonNull)
                .map(this::bitcoinAddressMapping)
    }


    private fun queryTxsByIds(ids: List<String>): List<BitcoinTransaction> {

        val statement = session.prepare("SELECT * FROM tx WHERE txid = ?")

        return ids.map { id -> session.executeAsync(statement.bind(id)) } // future<ResultSet>
                .map { futureResultSet ->
                    // cql row
                    while (!futureResultSet.isDone) Thread.sleep(10)
                    futureResultSet.get().one()
                }
                .filter(Objects::nonNull)
                .map(this::bitcoinTransactionMapping)
    }


    private fun bitcoinAddressMapping(row: Row): BitcoinAddress {
        return BitcoinAddress(
                id = row.getString("id"), balance = row.getString("balance"),
                tx_number = row.getInt("tx_number"), total_received = row.getString("total_received"),
                last_transaction_block = row.getLong("last_transaction_block")
        )
    }


    private fun bitcoinTransactionMapping(row: Row): BitcoinTransaction {
        return BitcoinTransaction(
                txid = row.getString("txid"), fee = row.getString("fee"), size = row.getInt("size"),
                block_number = row.getLong("block_number"),
                total_output = row.getString("total_output"), total_input = row.getString("total_input"),
                block_time = row.getTimestamp("block_time").toInstant().toString(),
                coinbase = row.getString("coinbase"), block_hash = row.getString("block_hash"),
                ins = row.getList("ins", BitcoinTransactionIn::class.java),
                outs = row.getList("outs", BitcoinTransactionOut::class.java)
        )
    }


    private fun bitcoinBlockMapping(row: Row): BitcoinBlock {
        return BitcoinBlock(
                height = row.getLong("height"), hash = row.getString("hash"), size = row.getInt("size"),
                time = row.getTimestamp("time").toInstant().toString(), nonce = row.getLong("nonce"),
                merkleroot = row.getString("merkleroot"), version = row.getInt("version"),
                weight = row.getInt("weight"), bits = row.getString("bits"), tx_number = row.getInt("tx_number"),
                total_outputs_value = row.getString("total_outputs_value"), difficulty = row.getVarint("difficulty")
        )
    }
}