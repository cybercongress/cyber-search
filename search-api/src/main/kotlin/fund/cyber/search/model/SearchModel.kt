package fund.cyber.search.model

import com.fasterxml.jackson.annotation.JsonRawValue
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity


data class ItemPreview(
        val chain: Chain,
        val entity: ChainEntity,
        @JsonRawValue val data: String
)


data class SearchResponse(

        val query: String,
        val page: Int,
        val pageSize: Int,

        val totalHits: Long,
        val searchTime: Long, //ms
        val items: List<ItemPreview>
)