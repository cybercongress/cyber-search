package fund.cyber.pump.cassandra

import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.node.common.awaitAll
import fund.cyber.node.model.CyberSearchItem
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.StorageAction
import fund.cyber.pump.StorageActionSourceFactory


class CassandraStorageAction(
        private val cassandraStorageAction: CassandraStorageActionSource,
        private val keyspaceRepository: CassandraKeyspaceRepository
) : StorageAction {

    override fun store() = cassandraStorageAction.store(keyspaceRepository)
    override fun remove() = cassandraStorageAction.remove(keyspaceRepository)
}


interface CassandraStorageActionSourceFactory<in T : BlockBundle> : StorageActionSourceFactory {
    fun constructCassandraAction(bundle: T): CassandraStorageActionSource
}

interface CassandraStorageActionSource {
    fun store(keyspaceRepository: CassandraKeyspaceRepository)
    fun remove(keyspaceRepository: CassandraKeyspaceRepository)
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

    override fun store(keyspaceRepository: CassandraKeyspaceRepository) {
        actions.forEach { action -> action.store(keyspaceRepository) }
    }

    override fun remove(keyspaceRepository: CassandraKeyspaceRepository) {
        actions.forEach { action -> action.remove(keyspaceRepository) }
    }
}


class StoreListCassandraStorageAction<out I : CyberSearchItem>(
        private val values: List<I>,
        private val valueType: Class<I>
) : CassandraStorageActionSource {

    override fun store(keyspaceRepository: CassandraKeyspaceRepository) {
        val mapper = keyspaceRepository.mappingManager.mapper(valueType)
        values.map { value -> mapper.saveAsync(value) }.awaitAll()
    }

    override fun remove(keyspaceRepository: CassandraKeyspaceRepository) {
        val mapper = keyspaceRepository.mappingManager.mapper(valueType)
        values.map { value -> mapper.saveAsync(value) }.awaitAll()
    }
}


class StoreValueCassandraStorageAction<out I : CyberSearchItem>(
        private val value: I,
        private val valueType: Class<I>
) : CassandraStorageActionSource {

    override fun store(keyspaceRepository: CassandraKeyspaceRepository) {
        keyspaceRepository.mappingManager.mapper(valueType).save(value)
    }

    override fun remove(keyspaceRepository: CassandraKeyspaceRepository) {
        keyspaceRepository.mappingManager.mapper(valueType).delete(value)
    }
}