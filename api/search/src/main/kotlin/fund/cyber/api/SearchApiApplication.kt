package fund.cyber.api

import fund.cyber.search.configuration.*
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import java.net.InetAddress


@SpringBootApplication
class SearchApiApplication {

    @Bean
    fun elasticClient(
            @Value("\${$CASSANDRA_HOSTS:$CASSANDRA_HOSTS_DEFAULT}") cassandraServers: String,
            @Value("\${$ELASTIC_TRANSPORT_PORT:$ELASTIC_TRANSPORT_PORT_DEFAULT}") elasticTransportPort: Int,
            @Value("\${$ELASTIC_CLUSTER_NAME:$ELASTIC_CLUSTER_NAME_DEFAULT}") elasticClusterName: String
    ): TransportClient {

        System.setProperty("es.set.netty.runtime.available.processors", "false")

        val elasticSettings = Settings.builder()
                .put("client.transport.sniff", true)
                .put("cluster.name", elasticClusterName)
                .build()!!

        return PreBuiltTransportClient(elasticSettings).apply {
            cassandraServers.split(",")
                    .map { server -> InetSocketTransportAddress(InetAddress.getByName(server), elasticTransportPort) }
                    .forEach { address -> addTransportAddress(address) }
        }
    }
}


fun main(args: Array<String>) {
    runApplication<SearchApiApplication>(*args)
}