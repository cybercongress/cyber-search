package fund.cyber.pump.ethereum_classic

import com.datastax.driver.mapping.Mapper
import fund.cyber.node.model.EthereumBlock
import fund.cyber.pump.StorageAction

class EthereumClassicCassandraAction<T>(block: T, mapper: Mapper<EthereumBlock>): StorageAction {
    private val block = block as EthereumBlock
    private val mapper = mapper

    override fun store() {
        println(this.mapper.saveQuery(block))
        this.mapper.save(block)
    }

    override fun remove() {

    }
}