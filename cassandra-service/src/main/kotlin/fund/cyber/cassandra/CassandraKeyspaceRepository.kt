package fund.cyber.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.common.Chain


open class CassandraKeyspaceRepository(cassandra: Cluster, cassandraKeyspace: String) {

    protected val session: Session by lazy { cassandra.connect(cassandraKeyspace) }
    val mappingManager by lazy { MappingManager(session) }
}

val Chain.keyspace: String
    get() = when (this) {
        Chain.BITCOIN -> "bitcoin"
        else -> this.toString().toLowerCase()
    }