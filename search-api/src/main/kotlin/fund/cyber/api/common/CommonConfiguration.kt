package fund.cyber.api.common

import fund.cyber.search.configuration.CASSANDRA_HOSTS
import fund.cyber.search.configuration.CASSANDRA_HOSTS_DEFAULT
import fund.cyber.search.configuration.CORS_ALLOWED_ORIGINS
import fund.cyber.search.configuration.CORS_ALLOWED_ORIGINS_DEFAULT
import fund.cyber.search.configuration.ELASTIC_CLUSTER_NAME
import fund.cyber.search.configuration.ELASTIC_CLUSTER_NAME_DEFAULT
import fund.cyber.search.configuration.ELASTIC_TRANSPORT_PORT
import fund.cyber.search.configuration.ELASTIC_TRANSPORT_PORT_DEFAULT
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.util.pattern.PathPatternParser
import java.net.InetAddress


fun <E : ServerResponse> List<RouterFunction<E>>.asSingleRouterFunction() = reduce { a, b -> a.and(b) }


@Configuration
class CommonConfiguration {

    @Value("\${$CORS_ALLOWED_ORIGINS:$CORS_ALLOWED_ORIGINS_DEFAULT}")
    private lateinit var allowedOrigin: String

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

    @Bean
    fun corsFilter(): CorsWebFilter {

        val config = CorsConfiguration()
        config.addAllowedOrigin(allowedOrigin)
        config.addAllowedHeader("*")
        config.addAllowedMethod("*")

        val source = UrlBasedCorsConfigurationSource(PathPatternParser())
        source.registerCorsConfiguration("/**", config)

        return CorsWebFilter(source)
    }
}
