package fund.cyber.dao.bitcoin

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.model.*


class BitcoinDaoService(private val cassandra: Cluster) {


    fun getBlockByNumber(number: Long): BitcoinBlock? {

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(BitcoinBlock::class.java)

        val resultSet = session.execute("SELECT * FROM bitcoin_block WHERE height=$number")

        return resultSet.map(this::bitcoinBlockMapping).firstOrNull()
    }


    fun getTxById(id: String): BitcoinTransaction? {

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(BitcoinTransaction::class.java)

        val resultSet = session.execute("SELECT * FROM bitcoin_tx WHERE txid='$id'")

        return resultSet.map(this::bitcoinTransactionMapping).firstOrNull()
    }


    fun getTxsByIds(ids: List<String>): List<BitcoinTransaction> {

        if (ids.isEmpty()) return emptyList()

        val txIds = ids.joinToString(separator = "','", postfix = "'", prefix = "'")

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(BitcoinTransaction::class.java)

        val resultSet = session.execute("SELECT * FROM bitcoin_tx WHERE txid in ($txIds)")

        return resultSet.map(this::bitcoinTransactionMapping)
    }


    private fun bitcoinTransactionMapping(row: Row): BitcoinTransaction {
        return BitcoinTransaction(
                txid = row.getString("txid"), fee = row.getString("fee"), size = row.getInt("size"),
                block_number = row.getLong("block_number"), lock_time = row.getLong("lock_time"),
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
                total_outputs_value = row.getString("total_outputs_value"), difficulty = row.getVarint("difficulty"),
                txs = row.getList("txs", BitcoinBlockTransaction::class.java)
        )
    }
}