package fund.cyber.address

import fund.cyber.cassandra.CassandraService
import fund.cyber.node.common.*


object ServiceContext {
    val cassandraService = CassandraService(ServiceConfiguration.cassandraServers, ServiceConfiguration.cassandraPort)

}


object ServiceConfiguration {

    val cassandraServers: List<String> = env(CASSANDRA_HOSTS, CASSANDRA_HOSTS_DEFAULT).split(",")
    val cassandraPort: Int = env(CASSANDRA_PORT, CASSANDRA_PORT_DEFAULT)

    val kafkaBrokers: List<String> = env(KAFKA_BROKERS, KAFKA_BROKERS_DEFAULT).split(",")
}