package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.Table
import java.util.*

@Table(name = "indexing_progress", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class IndexingProgress(
        val application_id: String,
        val block_number: Long,
        val block_hash: String,
        val index_time: Date
) {
    constructor() : this("", 0, "", Date())
}