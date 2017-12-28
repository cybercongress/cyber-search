package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import fund.cyber.search.configuration.AppContext
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest
import org.elasticsearch.client.transport.TransportClient


class IndexStatusHandler(
        private val jsonSerializer: ObjectMapper = AppContext.getJsonSerializer(),
        private val elasticClient: TransportClient = AppContext.elasticClient,
        private val indexToChainEntity: Map<String, Pair<Chain, ChainEntity>>
) : HttpHandler {

    private val request = IndicesStatsRequest().all()


    override fun handleRequest(exchange: HttpServerExchange) {


        val statsResponse = elasticClient.admin().indices().stats(request).actionGet()

        val response = statsResponse.indices
        val rawResponse = jsonSerializer.writeValueAsString(response)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}
