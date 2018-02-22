package fund.cyber.cassandra.common


interface CqlAddressSummary {
    val id: String
    val version: Long
    val kafka_delta_offset: Long
    val kafka_delta_partition: Int
    val kafka_delta_topic: String
    val kafka_delta_offset_committed: Boolean
}