package fund.cyber.search.model

import fund.cyber.node.model.ItemPreview

class SearchResponseItem

class SearchResponse(

        val query: String,
        val page: Int,
        val pageSize: Int,

        val totalHits: Int,
        val searchTime: Int, //ms
        val items: List<ItemPreview>
)