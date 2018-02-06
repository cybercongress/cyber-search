package fund.cyber.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager

const val FETCH_REQUEST_LIMIT_DEFAULT = 20

open class CassandraKeyspaceRepository(cassandra: Cluster, cassandraKeyspace: String) {

    protected val session: Session by lazy { cassandra.connect(cassandraKeyspace) }
    val mappingManager by lazy { MappingManager(session) }
}