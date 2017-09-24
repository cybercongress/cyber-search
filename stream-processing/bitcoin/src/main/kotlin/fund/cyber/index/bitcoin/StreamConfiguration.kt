package fund.cyber.index.bitcoin

import fund.cyber.node.common.env
import org.apache.kafka.streams.StreamsConfig
import java.util.*


class StreamConfiguration(
        val kafkaServers: String = env("KAFKA_CONNECTION", "localhost:9092"),
        val cassandraServers: String = env("CASSANDRA_CONNECTION", "localhost:9042"),
        val applicationId: String = "cyber.index.bitcoin.block.splitter"
) {
    fun streamProperties(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
        }
    }
}