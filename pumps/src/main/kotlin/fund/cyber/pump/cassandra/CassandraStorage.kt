package fund.cyber.pump.cassandra

import com.datastax.driver.mapping.MappingManager
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.StorageInterface
import fund.cyber.pump.ethereum_classic.EthereumClassic
import fund.cyber.pump.ethereum_classic.EthereumClassicBlock

import fund.cyber.pump.StoreAction
import fund.cyber.pump.ethereum_classic.EthereumClassicCassandraAction
import fund.cyber.pump.model.EthereumBlock
import fund.cyber.node.model.EthereumBlock as ModelEthereumBlock

class CassandraStorage: StorageInterface {

    private val connector: CassandraConnector = CassandraConnector()

    override fun initFor(blockchain: BlockchainInterface) {
        var keyspace: String? = null
        when(blockchain) {
            is EthereumClassic -> {
                keyspace = "ethereum_classic"
            }
        }
        this.connector.connect("127.0.0.1",  9042, keyspace)
       // this.connector.session?.cluster?.configuration?.codecRegistry?.register(TimestampAsStringCodec())
    }

    override fun <T>actionFor(block: T): StoreAction {
        val manager = MappingManager(this.connector.session)
        val ethBlock = EthereumBlock((block as EthereumClassicBlock).parityBlock)
        val mapper = manager.mapper(ModelEthereumBlock::class.java)
        return EthereumClassicCassandraAction(block = ethBlock, mapper = mapper)
    }
}