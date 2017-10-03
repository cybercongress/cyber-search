package fund.cyber.index.ethereum

import fund.cyber.index.ethereum.converter.EthereumParityToDaoConverter
import fund.cyber.node.common.env
import fund.cyber.node.kafka.JsonDeserializer
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.web3j.protocol.core.methods.response.EthBlock
import java.util.*

object ApplicationContext {

    val streamsConfiguration = StreamConfiguration()
    val parityToDaoConverter = EthereumParityToDaoConverter()
}

class StreamConfiguration(
        private val kafkaServers: String = env("KAFKA_CONNECTION", "localhost:9092"),
        private val applicationId: String = "cyber.index.ethereum.block.splitter"
) {
    fun streamProperties(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 10485760)
            put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760)
            put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000)
            put(StreamsConfig.STATE_DIR_CONFIG, "/opt/cyberfund/search/kafka-stream")
        }
    }
}

val parityBlockSerde = defaultJsonSerde(EthBlock.Block::class.java)
val ethereumTxSerde = defaultJsonSerde(EthereumTransaction::class.java)
val ethereumBlockSerde = defaultJsonSerde(EthereumBlock::class.java)

private fun <T> defaultJsonSerde(type: Class<T>): Serde<T> {
    return Serdes.serdeFrom(JsonSerializer(), JsonDeserializer(type))!!
}