package fund.cyber.api.search

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse
import org.elasticsearch.action.search.SearchResponse

fun SearchResponse.toCyberSearchResponse(query: String, page: Int, pageSize: Int)
    : fund.cyber.api.search.SearchResponse {

    val responseItems = this.hits.map { hit ->
        val chain = hit.index.substringBefore(".")
        val entity = hit.index.substringAfter(".")
        ItemPreview(chain, entity, hit.sourceAsString)
    }
    return SearchResponse(
        query = query, page = page, pageSize = pageSize, totalHits = this.hits.totalHits,
        items = responseItems, searchTime = this.tookInMillis
    )
}

fun IndicesStatsResponse.searchStats(): SearchStatsResponse {

    val indexSizeBytes = this.total.store.sizeInBytes

    val transactionsCount = this.indices
        .filterKeys { indexName -> indexName.endsWith("tx", true) }
        .values.map { txIndexStat -> txIndexStat.primaries.docs.count }.sum()

    val chains = this.indices.keys.map { indexName -> indexName.substringBefore(".") }.toSet()

    return SearchStatsResponse(chains.size, transactionsCount, indexSizeBytes)
}

fun IndicesStatsResponse.chainEntities(): Map<String, List<String>> {

    val chainsEntities = mutableMapOf<String, MutableList<String>>()

    this.indices.keys.forEach { indexName ->

        val chainName = indexName.substringBefore(".")
        val entityName = indexName.substringAfter(".")

        if (chainsEntities.containsKey(chainName)) {
            chainsEntities[chainName]!!.add(entityName)
        } else {
            chainsEntities[chainName] = mutableListOf(entityName)
        }
    }
    return chainsEntities
}
