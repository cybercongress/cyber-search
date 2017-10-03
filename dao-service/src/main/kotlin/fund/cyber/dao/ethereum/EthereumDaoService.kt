package fund.cyber.dao.ethereum

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumBlockTransaction
import fund.cyber.node.model.EthereumTransaction

class EthereumDaoService(private val cassandra: Cluster) {


    fun getBlockByNumber(number: Long): EthereumBlock? {

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(EthereumBlock::class.java)

        val resultSet = session.execute("SELECT * FROM ethereum_block WHERE number=$number")

        return resultSet.map(this::ethereumBlockMapping).firstOrNull()
    }


    fun getTxByHash(hash: String): EthereumTransaction? {

        val session = cassandra.connect("blockchains")
        val manager = MappingManager(session)
        val mapper = manager.mapper(EthereumTransaction::class.java)

        val resultSet = session.execute("SELECT * FROM ethereum_tx WHERE hash='$hash'")

        return resultSet.map(this::ethereumTransactionMapping).firstOrNull()
    }

    private fun ethereumTransactionMapping(row: Row): EthereumTransaction {
        return EthereumTransaction(
                hash = row.getString("hash"), block_hash = row.getString("block_hash"),
                nonce = row.getLong("nonce"), block_number = row.getLong("block_number"),
                timestamp = row.getTimestamp("timestamp").toInstant().toString(),
                transaction_index = row.getLong("transaction_index"), from = row.getString("from"),
                to = row.getString("to"), value = row.getString("value"),
                gas_price = row.getDecimal("gas_price"), gas_limit = row.getLong("gas_limit"),
                gas_used = row.getLong("gas_used"), input = row.getString("input"),
                creates = row.getString("creates"), fee = row.getString("fee")
        )
    }

    private fun ethereumBlockMapping(row: Row): EthereumBlock {
        return EthereumBlock(
                number = row.getLong("number"), hash = row.getString("hash"),
                size = row.getLong("size"), parent_hash = row.getString("parent_hash"),
                timestamp = row.getTimestamp("timestamp").toInstant().toString(),
                sha3_uncles = row.getString("sha3_uncles"), logs_bloom = row.getString("logs_bloom"),
                transactions_root = row.getString("transactions_root"), state_root = row.getString("state_root"),
                receipts_root = row.getString("receipts_root"), miner = row.getString("miner"),
                difficulty = row.getVarint("difficulty"), total_difficulty = row.getVarint("total_difficulty"),
                extra_data = row.getString("extra_data"), uncles = row.getList("uncles", String::class.java),
                gas_used = row.getLong("gas_used"), gas_limit = row.getLong("gas_limit"),
                tx_number = row.getInt("tx_number"),
                transactions = row.getList("transactions", EthereumBlockTransaction::class.java)
        )
    }
}