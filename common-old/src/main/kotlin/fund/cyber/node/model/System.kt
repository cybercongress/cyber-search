package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import java.util.*

@Table(keyspace = "cyber_system", name = "schema_version", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class SchemaVersion(
        val application_id: String,
        val version: Int,
        val migration_hash: Int,
        val apply_time: Date
) {
    constructor() : this("", 0, 0, Date())
}

abstract class CyberSearchItem