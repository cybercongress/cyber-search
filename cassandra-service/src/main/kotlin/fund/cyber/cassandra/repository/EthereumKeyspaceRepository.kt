package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.cassandra.keyspace
import fund.cyber.node.common.Chain
import fund.cyber.node.model.EthereumAddress
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import org.ehcache.Cache
import java.util.*


class EthereumKeyspaceRepository(
        cassandra: Cluster, chain: Chain
) : CassandraKeyspaceRepository(cassandra, chain.keyspace) {


    private val addressCache: Cache<String, EthereumAddress>? = null

    fun getAddress(id: String): EthereumAddress? {

        val resultSet = session.execute("SELECT * FROM address WHERE id='$id'")
        return resultSet.map(this::ethereumAddressMapping).firstOrNull()
    }


    fun getBlockByNumber(number: Long): EthereumBlock? {

        val resultSet = session.execute("SELECT * FROM block WHERE number=$number")
        return resultSet.map(this::ethereumBlockMapping).firstOrNull()
    }


    fun getTxByHash(hash: String): EthereumTransaction? {

        val resultSet = session.execute("SELECT * FROM tx WHERE hash='$hash'")
        return resultSet.map(this::ethereumTransactionMapping).firstOrNull()
    }


    fun getAddressesWithLastTransactionBeforeGivenBlock(ids: List<String>, blockNumber: Long): List<EthereumAddress> {

        if (ids.isEmpty()) return emptyList()

        return when (addressCache) {
            null -> queryAddressesWithLastTransactionBeforeGivenBlock(ids, blockNumber)
            else -> {
                val addresses = mutableListOf<EthereumAddress>()
                val idsWithoutCacheHit = mutableListOf<String>()

                for (id in ids) {

                    val address = addressCache[id]
                    if (address != null && address.last_transaction_block < blockNumber)
                        addresses.add(address)
                    else
                        idsWithoutCacheHit.add(id)
                }

                addresses.addAll(queryAddressesWithLastTransactionBeforeGivenBlock(idsWithoutCacheHit, blockNumber))
                return addresses
            }
        }
    }


    private fun queryAddressesWithLastTransactionBeforeGivenBlock(
            ids: List<String>, blockNumber: Long): List<EthereumAddress> {

        val statement = session.prepare(
                "SELECT * FROM address WHERE id=? AND last_transaction_block < $blockNumber ALLOW FILTERING"
        )

        return ids.map { id -> session.executeAsync(statement.bind(id)) } // future<ResultSet>
                .map { futureResultSet ->
                    // cql row
                    while (!futureResultSet.isDone) Thread.sleep(10)
                    futureResultSet.get().one()
                }
                .filter(Objects::nonNull)
                .map(this::ethereumAddressMapping)
    }


    private fun ethereumAddressMapping(row: Row): EthereumAddress {
        return EthereumAddress(
                id = row.getString("id"), balance = row.getString("balance"),
                contract_address = row.getBool("contract_address"), tx_number = row.getInt("tx_number"),
                total_received = row.getString("total_received"),
                last_transaction_block = row.getLong("last_transaction_block")
        )
    }


    private fun ethereumTransactionMapping(row: Row): EthereumTransaction {
        return EthereumTransaction(
                hash = row.getString("hash"), block_hash = row.getString("block_hash"),
                nonce = row.getLong("nonce"), block_number = row.getLong("block_number"),
                timestamp = row.getTimestamp("timestamp").toInstant(),
                transaction_index = row.getLong("transaction_index"), from = row.getString("from"),
                to = row.getString("to"), value = row.getString("value"),
                gas_price = row.getDecimal("gas_price"), gas_limit = row.getLong("gas_limit"),
                gas_used = row.getLong("gas_used"), input = row.getString("input"),
                creates = row.getString("creates"), fee = row.getString("fee")
        )
    }


    private fun ethereumBlockMapping(row: Row): EthereumBlock {
        return EthereumBlock(
                number = row.getLong("number"), hash = row.getString("hash"), block_reward = row.getString("block_reward"),
                size = row.getLong("size"), parent_hash = row.getString("parent_hash"),
                timestamp = row.getTimestamp("timestamp").toInstant(),
                sha3_uncles = row.getString("sha3_uncles"), logs_bloom = row.getString("logs_bloom"),
                transactions_root = row.getString("transactions_root"), state_root = row.getString("state_root"),
                receipts_root = row.getString("receipts_root"), miner = row.getString("miner"),
                difficulty = row.getVarint("difficulty"), total_difficulty = row.getVarint("total_difficulty"),
                extra_data = row.getString("extra_data"), uncles = row.getList("uncles", String::class.java),
                gas_used = row.getLong("gas_used"), gas_limit = row.getLong("gas_limit"),
                tx_number = row.getInt("tx_number"), tx_fees = row.getString("tx_fees")
        )
    }
}