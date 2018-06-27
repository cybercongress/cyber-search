package fund.cyber.common.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

const val KAFKA_MAX_MESSAGE_SIZE_BYTES = 15728640
const val SESSION_TIMEOUT_MS_CONFIG = 30000
const val DEFAULT_POLL_TIMEOUT = 5000L

fun defaultConsumerConfig() = mutableMapOf<String, Any>(
    ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG to KAFKA_MAX_MESSAGE_SIZE_BYTES,
    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG to SESSION_TIMEOUT_MS_CONFIG
)


fun idempotentProducerDefaultConfig() = mutableMapOf(
    ProducerConfig.MAX_REQUEST_SIZE_CONFIG to KAFKA_MAX_MESSAGE_SIZE_BYTES,
    /* This settings guarantee exactly once, in order delivery per partition for topics with replication-factor >= 2,
       and assuming that the system doesn't suffer multiple hard failures or concurrent transient failures.
       https://bit.ly/2IjnHxl */
    ProducerConfig.ACKS_CONFIG to "all",
    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 2,
    ProducerConfig.RETRIES_CONFIG to Int.MAX_VALUE,
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true
)

fun defaultProducerConfig() = mutableMapOf(
    ProducerConfig.MAX_REQUEST_SIZE_CONFIG to KAFKA_MAX_MESSAGE_SIZE_BYTES,
    ProducerConfig.ACKS_CONFIG to "all"
)
