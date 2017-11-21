package fund.cyber.pump.cassandra

import fund.cyber.pump.Block
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.StorageInterface
import fund.cyber.pump.ethereum_classic.EthereumClassic
import fund.cyber.pump.ethereum_classic.EthereumClassicBlock

import com.fasterxml.jackson.module.kotlin.*

class CassandraStorage: StorageInterface {

    private val connector: CassandraConnector = CassandraConnector()
    private var domain: CassandraDomain? = null

    override fun initFor(blockchain: BlockchainInterface) {
        this.connector.connect("127.0.0.1",  9042)
        when(blockchain) {
            is EthereumClassic -> this.domain = CassandraEthereumClassicDomain()
        }

    }

    override fun store(block: Block) {
        val saveBlock = this.domain?.storeBlock(block)
        println(saveBlock)
        this.connector.session?.execute(saveBlock)
    }
}

interface CassandraDomain {
    fun createTables(): String
    fun storeBlock(block: Block): String
}

class CassandraEthereumClassicDomain: CassandraDomain {
    override fun createTables(): String {
        val inputStream = CassandraEthereumClassicDomain::class.java.getResourceAsStream("/ethereumClassic.cql")

        return inputStream.bufferedReader().use { it.readText() }
    }

    override fun storeBlock(block: Block): String {
        if (block !is EthereumClassicBlock) return ""

        val ethBlock = block.parityBlock.block

        val query = "INSERT INTO ethereum_classic.block (number, hash, parent_hash, timestamp) VALUES (${ethBlock.number}, '${ethBlock.hash}', '${ethBlock.parentHash}', ${ethBlock.timestamp});"
        return query
    }
}