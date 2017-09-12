package fund.cyber.search.handler

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.node.common.intValue
import fund.cyber.node.common.stringValue
import fund.cyber.search.context.AppContext
import fund.cyber.search.model.SearchResponse
import io.undertow.server.HttpHandler
import io.undertow.server.HttpServerExchange
import io.undertow.util.Headers


class SearchHandler(
        private val jsonSerializer: ObjectMapper = AppContext.jsonSerializer
) : HttpHandler {

    override fun handleRequest(exchange: HttpServerExchange) {

        val query = exchange.queryParameters["query"]?.stringValue()
        val page = exchange.queryParameters["page"]?.intValue() ?: 0
        val pageSize = exchange.queryParameters["pageSize"]?.intValue() ?: 10

        if (query == null || query.isEmpty()) {
            exchange.statusCode = 400
            return
        }

        val response = SearchResponse(
                query = query, page = page, pageSize = pageSize,
                items = emptyList(), searchTime = 523, totalHits = 0
        )
        val rawResponse = jsonSerializer.writeValueAsString(response)

        exchange.responseHeaders.put(Headers.CONTENT_TYPE, "application/json")
        exchange.responseSender.send(rawResponse)
    }
}
