package fund.cyber.node.kafka


enum class PumpEvent {
    NEW_BLOCK,
    NEW_POOL_TX,
    DROPPED_BLOCK;
}