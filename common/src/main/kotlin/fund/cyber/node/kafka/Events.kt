package fund.cyber.node.kafka


enum class KafkaEvent {
    NEW_BLOCK_TX,
    NEW_POOL_TX,
    DROPPED_BLOCK_TX;
}