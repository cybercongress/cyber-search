package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import com.google.common.util.concurrent.ListenableFuture
import fund.cyber.cassandra.PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST
import fund.cyber.cassandra.model.keyspace
import fund.cyber.node.common.Chain
import fund.cyber.node.common.awaitAll
import fund.cyber.node.model.CyberSearchItem
import io.reactivex.Flowable

interface ChainRepository<in T : CyberSearchItem> {

    fun save(entity: T)
    fun save(entities: List<T>)
    fun saveAsync(entity: T): ListenableFuture<Void>

    fun delete(entity: T)
    fun deleteAsync(entity: T): ListenableFuture<Void>

    fun <R : T> get(vararg arguments: Any, entityType: Class<R>): R
    fun <R : T> getAsync(vararg arguments: Any, entityType: Class<R>): ListenableFuture<R>
}


@Suppress("UNCHECKED_CAST")
class BasicChainRepository<in T : CyberSearchItem>(cassandra: Cluster, chain: Chain) : ChainRepository<T> {

    private val session: Session by lazy { cassandra.connect(chain.keyspace) }
    private val mappingManager by lazy { MappingManager(session) }

    override fun save(entity: T) = mappingManager.mapper(entity::class.java as Class<T>).save(entity)
    override fun saveAsync(entity: T) = mappingManager.mapper(entity::class.java as Class<T>).saveAsync(entity)!!

    override fun save(entities: List<T>) {
        Flowable.fromIterable(entities)
                .buffer(PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST)
                .blockingForEach { entitiesChunk ->
                    entitiesChunk.map { entity -> saveAsync(entity) }.awaitAll()
                }
    }

    override fun <R : T> get(vararg arguments: Any, entityType: Class<R>): R
            = mappingManager.mapper(entityType).get(arguments)

    override fun <R : T> getAsync(vararg arguments: Any, entityType: Class<R>): ListenableFuture<R>
            = mappingManager.mapper(entityType).getAsync(arguments)!!

    override fun delete(entity: T) = mappingManager.mapper(entity::class.java as Class<T>).delete(entity)

    override fun deleteAsync(entity: T): ListenableFuture<Void>
            = mappingManager.mapper(entity::class.java as Class<T>).deleteAsync(entity)!!
}