package fund.cyber.api.search

import fund.cyber.search.model.ItemPreview
import fund.cyber.search.model.SearchResponse
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.chains.BitcoinFamilyChainEntity
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.Fuzziness
import org.elasticsearch.index.query.QueryBuilders
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
class SearchController(
        private val elasticClient: TransportClient
) {

    //todo slice caching, general web caching
    @GetMapping("/search")
    fun search(
            @RequestParam query: String,
            @RequestParam(required = false, defaultValue = "0") page: Int,
            @RequestParam(required = false, defaultValue = "10") pageSize: Int
    ): Mono<SearchResponse> {

        val elasticQuery = QueryBuilders.matchQuery("_all", query)
                .fuzziness(Fuzziness.ZERO)

        val elasticResponse = elasticClient.prepareSearch()
                .setQuery(elasticQuery)
                .setFrom(page * pageSize).setSize(pageSize).setExplain(true)
                .get()

        val responseItems = elasticResponse.hits.map { hit ->
            ItemPreview(BitcoinFamilyChain.BITCOIN, BitcoinFamilyChainEntity.TRANSACTION, hit.sourceAsString)
        }

        return Mono.just(
                SearchResponse(
                        query = query, page = page, pageSize = pageSize, totalHits = elasticResponse.hits.totalHits,
                        items = responseItems, searchTime = elasticResponse.tookInMillis
                )
        )
    }
}