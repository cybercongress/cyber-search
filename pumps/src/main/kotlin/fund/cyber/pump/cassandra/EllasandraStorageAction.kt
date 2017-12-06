package fund.cyber.pump.cassandra

import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.common.awaitAll
import fund.cyber.node.model.CyberSearchItem
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.StorageAction
import fund.cyber.pump.StorageActionFactory

interface CassandraStorageActionFactory<in T : BlockBundle> : StorageActionFactory {
    fun constructCassandraAction(bundle: T): CassandraStorageAction
}

interface CassandraStorageAction {
    fun store(session: Session, mappingManager: MappingManager)
    fun remove(session: Session, mappingManager: MappingManager)
}

class SimpleCassandraStorageAction(
        private val cassandraStorageAction: CassandraStorageAction,
        private val session: Session,
        private val mappingManager: MappingManager
) : StorageAction {

    override fun store() = cassandraStorageAction.store(session, mappingManager)
    override fun remove() = cassandraStorageAction.remove(session, mappingManager)
}


class CompositeCassandraStorageAction(
        private vararg val actions: CassandraStorageAction
) : CassandraStorageAction {

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
) : CassandraStorageAction {

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
) : CassandraStorageAction {

    override fun store(session: Session, mappingManager: MappingManager) {
        mappingManager.mapper(valueType).save(value)
    }

    override fun remove(session: Session, mappingManager: MappingManager) {
        mappingManager.mapper(valueType).delete(value)
    }

}