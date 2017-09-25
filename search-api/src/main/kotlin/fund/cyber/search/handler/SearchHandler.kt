package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.common.intValue
import fund.cyber.node.common.stringValue
import fund.cyber.node.model.ItemPreview
import fund.cyber.search.configuration.SearchApiConfiguration
import fund.cyber.search.context.AppContext
import fund.cyber.search.model.SearchResponse
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers
import java.net.InetAddress
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.common.unit.Fuzziness


class SearchHandler(
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer,
        configuration: SearchApiConfiguration = SearchApiConfiguration()
) : HttpHandler {

    var settings = Settings.settingsBuilder().put("cluster.name", configuration.elasticClusterName).build()

    var client = TransportClient.builder().settings(settings).build()
            .addTransportAddress(InetSocketTransportAddress(
                    InetAddress.getByName(configuration.elasticHost), configuration.elasticPort)
            )


    override fun handleRequest(exchange: HttpServerExchange) {

        val query = exchange.queryParameters["query"]?.stringValue()
        val page = exchange.queryParameters["page"]?.intValue() ?: 0
        val pageSize = exchange.queryParameters["pageSize"]?.intValue() ?: 10

        if (query == null || query.isEmpty()) {
            exchange.statusCode = 400
            return
        }

        val elasticQuery = QueryBuilders.matchQuery("_all", query)
                .fuzziness(Fuzziness.fromEdits(2))

        val elasticResponse = client.prepareSearch("blockchains")
                .setQuery(elasticQuery)
                .setFrom(page * pageSize).setSize(pageSize).setExplain(true)
                .execute()
                .actionGet()


        val responseItems = elasticResponse.hits
                .map { hit -> ItemPreview(type = hit.type, data = hit.sourceAsString()) }

        val response = SearchResponse(
                query = query, page = page, pageSize = pageSize,
                items = responseItems, searchTime = elasticResponse.tookInMillis, totalHits = elasticResponse.hits.totalHits
        )
        val rawResponse = jsonSerializer.writeValueAsString(response)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}
