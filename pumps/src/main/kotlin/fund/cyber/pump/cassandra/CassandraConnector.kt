package fund.cyber.pump.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager


class CassandraConnector {

    private var cluster: Cluster? = null

    var session: Session? = null
        private set

    var manager: MappingManager? = null

    fun connect(node: String, port: Int?, keyspace: String?) {
        val b = Cluster.builder().addContactPoint(node)
        if (port != null) {
            b.withPort(port)
        }
        cluster = b.build()

//        if (keyspace == null) {
//            session =  cluster!!.connect()
//        }else {
            session = cluster!!.connect(keyspace)
//        }

        if (session != null) {
            manager = MappingManager(session)
        }
    }

    fun close() {
        session!!.close()
        cluster!!.close()
    }
}
