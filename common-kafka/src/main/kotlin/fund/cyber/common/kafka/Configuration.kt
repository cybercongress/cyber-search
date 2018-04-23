package fund.cyber.common.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

const val KAFKA_MAX_MESSAGE_SIZE_BYTES = 15728640
const val SESSION_TIMEOUT_MS_CONFIG = 30000

fun defaultConsumerConfig() = mutableMapOf<String,Any>(
        ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG to KAFKA_MAX_MESSAGE_SIZE_BYTES,
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to SESSION_TIMEOUT_MS_CONFIG
)

fun defaultProducerConfig() = mutableMapOf<String,Any>(
        ProducerConfig.MAX_REQUEST_SIZE_CONFIG to KAFKA_MAX_MESSAGE_SIZE_BYTES
)


