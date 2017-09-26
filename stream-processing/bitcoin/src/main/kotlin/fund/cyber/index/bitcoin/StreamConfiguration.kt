package fund.cyber.index.bitcoin

import fund.cyber.node.common.env
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import java.util.*


class StreamConfiguration(
        val kafkaServers: String = env("KAFKA_CONNECTION", "localhost:9092"),
        val cassandraServers: String = env("CASSANDRA_CONNECTION", "127.0.0.1"),
        val applicationId: String = "cyber.index.bitcoin.block.splitter1"
) {
    fun streamProperties(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760)
            put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000)
            put(StreamsConfig.STATE_DIR_CONFIG, "/opt/cyberfund/search/kafka-stream")
        }
    }
}