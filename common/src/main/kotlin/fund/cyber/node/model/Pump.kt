package fund.cyber.node.model

import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import java.util.*


const val TX_MEMORY_POOL_BLOCK_HASH = "Memory Pool"
const val TX_MEMORY_POOL_BLOCK_NUMBER = Long.MAX_VALUE

@Table(name = "indexing_progress", readConsistency = "QUORUM", writeConsistency = "QUORUM")
data class IndexingProgress(
        @PartitionKey val application_id: String,
        val block_number: Long,
        val block_hash: String,
        val index_time: Date
) {
    constructor() : this("", 0, "", Date())
}