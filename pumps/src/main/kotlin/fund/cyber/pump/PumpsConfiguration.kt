package fund.cyber.pump

import com.datastax.driver.core.Cluster
import fund.cyber.node.common.*
import java.util.*


object AppContext {

    val pumpsConfiguration = PumpsConfiguration()

    val cassandra = Cluster.builder()
            .addContactPoints(*pumpsConfiguration.cassandraServers.toTypedArray())
            .withPort(pumpsConfiguration.cassandraPort)
            .withMaxSchemaAgreementWaitSeconds(30)
            .build().init()
}


class PumpsConfiguration(
        val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(","),
        val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT),
        val processLastBlock: Long = env("CS_LAST_PROCESSED_BLOCK", -1)
)