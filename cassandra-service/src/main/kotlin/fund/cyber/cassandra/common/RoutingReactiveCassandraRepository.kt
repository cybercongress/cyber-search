package fund.cyber.cassandra.common

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Metadata
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import fund.cyber.search.model.chains.ChainInfo
import org.reactivestreams.Publisher
import org.springframework.data.cassandra.core.ReactiveCassandraOperations
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactory
import org.springframework.data.cassandra.repository.support.ReactiveCassandraRepositoryFactoryBean
import org.springframework.data.cassandra.repository.support.SimpleReactiveCassandraRepository
import org.springframework.data.repository.NoRepositoryBean
import org.springframework.data.repository.Repository
import org.springframework.data.repository.core.RepositoryInformation
import org.springframework.data.repository.core.RepositoryMetadata
import org.springframework.data.repository.core.support.RepositoryFactorySupport
import org.springframework.util.Assert
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.*

@NoRepositoryBean
interface RoutingReactiveCassandraRepository<S, ID> : ReactiveCassandraRepository<S, ID>


@NoRepositoryBean
class RoutingReactiveCassandraRepositoryImpl<S, ID>(
    private val metadata: CassandraEntityInformation<S, ID>,
    private val operations: ReactiveCassandraOperations,
    private val clusterMetadata: Metadata,
    private val keyspace: String
) : SimpleReactiveCassandraRepository<S, ID>(metadata, operations), RoutingReactiveCassandraRepository<S, ID> {


    override fun <E : S> save(entity: E): Mono<E> {

        Assert.notNull(entity, "Entity must not be null")
        return operations.reactiveCqlOperations.execute(createInsertWithRoutingKey(entity as Any)).map { entity }
    }

    override fun <E : S> saveAll(entityStream: Publisher<E>): Flux<E> {

        Assert.notNull(entityStream, "The given Publisher of entities must not be null");

        return Flux.from(entityStream)
            .flatMap { entity ->
                operations.reactiveCqlOperations.execute(createInsertWithRoutingKey(entity as Any)).map { entity }
            }
    }

    override fun findById(id: ID): Mono<S> {
        val entityClass = metadata.javaType

        Assert.notNull(id, "Id must not be null");
        Assert.notNull(entityClass, "Entity type must not be null");

        val entity = operations.converter.mappingContext.getRequiredPersistentEntity(entityClass)

        val select = QueryBuilder.select().all().from(entity.tableMetadata())

        operations.converter.write(id as Any, select.where(), entity)

        return operations.reactiveCqlOperations
            .query(select) { row, _ -> operations.converter.read(entityClass, row) }.next()
    }

    private fun createInsertWithRoutingKey(entity: Any): Insert {

        val persistentEntity = operations.converter.mappingContext
            .getRequiredPersistentEntity(entity::class.java)

        val toInsert = LinkedHashMap<String, Any>()

        operations.converter.write(entity, toInsert, persistentEntity)

        val tableMetadata = persistentEntity.tableMetadata()

        val insert = QueryBuilder.insertInto(tableMetadata)

        for ((key, value) in toInsert) {
            insert.value(key, value)
        }

        return insert
    }

    private fun BasicCassandraPersistentEntity<*>.tableMetadata() =
        clusterMetadata.getKeyspace(keyspace).getTable(this.tableName.toCql())!!

}


class RoutingReactiveCassandraRepositoryFactoryBean<T : Repository<S, ID>, S, ID>(
    repositoryInterface: Class<out T>,
    private val cluster: Cluster,
    private val chainInfo: ChainInfo
) : ReactiveCassandraRepositoryFactoryBean<T, S, ID>(repositoryInterface) {

    override fun getFactoryInstance(operations: ReactiveCassandraOperations): RepositoryFactorySupport {
        return RoutingReactiveCassandraRepositoryFactory<S, ID>(operations, cluster.metadata, chainInfo.keyspace)
    }

    class RoutingReactiveCassandraRepositoryFactory<S, ID>(
        private val cassandraOperations: ReactiveCassandraOperations,
        private val metadata: Metadata,
        private val keyspace: String
    ) : ReactiveCassandraRepositoryFactory(cassandraOperations) {

        override fun getTargetRepository(information: RepositoryInformation): Any {
            val entityInformation = getEntityInformation<S, ID>(information.domainType as Class<S>)
            return RoutingReactiveCassandraRepositoryImpl<S, ID>(
                entityInformation, cassandraOperations, metadata, keyspace
            )
        }

        override fun getRepositoryBaseClass(metadata: RepositoryMetadata): Class<*> {
            return RoutingReactiveCassandraRepository::class.java
        }
    }

}
