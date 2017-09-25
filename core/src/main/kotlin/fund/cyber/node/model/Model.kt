package fund.cyber.node.model

import com.fasterxml.jackson.annotation.JsonRawValue


data class ItemPreview(
        val type: String,
        @JsonRawValue val data: String
)

data class Block(
        val chunk_id: String,
        val number: String,
        val rawBlock: String
)