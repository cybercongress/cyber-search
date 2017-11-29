package fund.cyber.pump.ethereum

import com.datastax.driver.mapping.Mapper
import fund.cyber.dao.ethereum.EthereumDaoService
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.EthereumTxPreviewByBlock
import fund.cyber.pump.StorageAction

class EthereumCassandraSaveAction(bundle: EthereumBlockBundle, daoService: EthereumDaoService): StorageAction {
    val action: CassandraAction<EthereumBlock>
    init {
        val manager = daoService.manager
        val mapperTx = manager.mapper(EthereumTransaction::class.java)
        val mapperTxByBlock = manager.mapper(EthereumTxPreviewByBlock::class.java)
        action = CassandraAction(bundle.block, manager.mapper(EthereumBlock::class.java))

        action.dependencies += bundle.transactions
                .fold(listOf<StorageAction>()) { res, it ->
                    res +
                            CassandraAction(it, mapperTx) +
                            CassandraAction(EthereumTxPreviewByBlock(it), mapperTxByBlock)
                }
    }
    override fun store() {
        action.store()
    }

    override fun remove() {
        action.remove()
    }

}

class CassandraAction<T>(obj: T, mapper: Mapper<T>): StorageAction {
    val obj = obj
    private val mapper = mapper
    var dependencies: List<StorageAction> = listOf()

    override fun store() {
        for (sa in this.dependencies) {
            sa.store()
        }
//        println(this.mapper.saveQuery(obj))
        this.mapper.save(obj)
    }

    override fun remove() {
        for (sa in this.dependencies) {
            sa.remove()
        }
        this.mapper.delete(this.obj)
    }
}