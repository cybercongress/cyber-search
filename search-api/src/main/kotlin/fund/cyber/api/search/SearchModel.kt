package fund.cyber.api.search

import com.fasterxml.jackson.annotation.JsonRawValue


data class ItemPreview(
        val chain: String,
        val entity: String,
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
