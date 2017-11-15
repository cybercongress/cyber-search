package fund.cyber.pump.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

class CassandraConnector {

    private var cluster: Cluster? = null

    var session: Session? = null
        private set

    fun connect(node: String, port: Int?) {
        val b = Cluster.builder().addContactPoint(node)
        if (port != null) {
            b.withPort(port)
        }
        cluster = b.build()

        session = cluster!!.connect()
    }

    fun close() {
        session!!.close()
        cluster!!.close()
    }
}
