package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table

@Table(keyspace = "cyber_system", name = "schema_version",
        readConsistency = "QUORUM", writeConsistency = "QUORUM",
        caseSensitiveKeyspace = false, caseSensitiveTable = false)
data class SchemaVersion(
        val application_id: String,
        val version: Int,
        val migration_hash: Int,
        val apply_time: String
)