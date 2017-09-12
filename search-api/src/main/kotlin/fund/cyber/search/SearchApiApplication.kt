package fund.cyber.search

import fund.cyber.search.handler.SearchHandler
import io.undertow.Handlers
import io.undertow.Undertow


object SearchApiApplication {

    @JvmStatic
    fun main(args: Array<String>) {

        val httpHandler = Handlers.path()
                .addPrefixPath("/search", SearchHandler())

        Undertow.builder()
                .addHttpListener(8085, "0.0.0.0")
                .setHandler(httpHandler)
                .build().start()
    }
}