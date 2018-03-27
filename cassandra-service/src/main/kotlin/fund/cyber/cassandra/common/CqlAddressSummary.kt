package fund.cyber.cassandra.common


interface CqlAddressSummary {
    val id: String
    val version: Long
    val kafkaDeltaOffset: Long
    val kafkaDeltaPartition: Int
    val kafkaDeltaTopic: String
    val kafkaDeltaOffsetCommitted: Boolean
}
