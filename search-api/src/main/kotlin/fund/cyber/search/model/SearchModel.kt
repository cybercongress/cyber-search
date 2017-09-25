package fund.cyber.search.model

import fund.cyber.node.model.ItemPreview

class SearchResponseItem

class SearchResponse(

        val query: String,
        val page: Int,
        val pageSize: Int,

        val totalHits: Long,
        val searchTime: Long, //ms
        val items: List<ItemPreview>
)