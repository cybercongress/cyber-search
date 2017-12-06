package fund.cyber.pump.cassandra

import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.common.awaitAll
import fund.cyber.node.model.CyberSearchItem
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.StorageAction
import fund.cyber.pump.StorageActionSourceFactory


class CassandraStorageAction(
        private val cassandraStorageAction: CassandraStorageActionSource,
        private val session: Session,
        private val mappingManager: MappingManager
) : StorageAction {

    override fun store() = cassandraStorageAction.store(session, mappingManager)
    override fun remove() = cassandraStorageAction.remove(session, mappingManager)
}


interface CassandraStorageActionSourceFactory<in T : BlockBundle> : StorageActionSourceFactory {
    fun constructCassandraAction(bundle: T): CassandraStorageActionSource
}

interface CassandraStorageActionSource {
    fun store(session: Session, mappingManager: MappingManager)
    fun remove(session: Session, mappingManager: MappingManager)
}


class SimpleCassandraActionSourceFactory : CassandraStorageActionSourceFactory<BlockBundle> {

    override fun constructCassandraAction(bundle: BlockBundle): CassandraStorageActionSource {
        val actions = bundle.elementsMap().map { (type, values) -> StoreListCassandraStorageAction(values, type) }
        return CompositeCassandraStorageAction(*actions.toTypedArray())
    }
}


class CompositeCassandraStorageAction(
        private vararg val actions: CassandraStorageActionSource
) : CassandraStorageActionSource {

    override fun store(session: Session, mappingManager: MappingManager) {
        actions.forEach { action -> action.store(session, mappingManager) }
    }

    override fun remove(session: Session, mappingManager: MappingManager) {
        actions.forEach { action -> action.remove(session, mappingManager) }
    }
}


class StoreListCassandraStorageAction<out I : CyberSearchItem>(
        private val values: List<I>,
        private val valueType: Class<I>
) : CassandraStorageActionSource {

    override fun store(session: Session, mappingManager: MappingManager) {
        val mapper = mappingManager.mapper(valueType)
        values.map { value -> mapper.saveAsync(value) }.awaitAll()
    }

    override fun remove(session: Session, mappingManager: MappingManager) {
        val mapper = mappingManager.mapper(valueType)
        values.map { value -> mapper.saveAsync(value) }.awaitAll()
    }
}


class StoreValueCassandraStorageAction<out I : CyberSearchItem>(
        private val value: I,
        private val valueType: Class<I>
) : CassandraStorageActionSource {

    override fun store(session: Session, mappingManager: MappingManager) {
        mappingManager.mapper(valueType).save(value)
    }

    override fun remove(session: Session, mappingManager: MappingManager) {
        mappingManager.mapper(valueType).delete(value)
    }

}