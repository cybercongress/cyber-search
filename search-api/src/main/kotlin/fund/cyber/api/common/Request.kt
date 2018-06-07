package fund.cyber.api.common

import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

inline fun <reified T> ServerRequest.toPageableResponse(getPage: (Pageable) -> Slice<T>): Mono<ServerResponse> {
    val page = this.queryParam("page").orElse("0").toInt()
    val pageSize = this.queryParam("pageSize").orElse("20").toInt()

    var slice = getPage(CassandraPageRequest.first(pageSize))

    for (i in 1..page) {
        if (slice.hasNext()) {
            slice = getPage(slice.nextPageable())
        } else return ServerResponse.notFound().build()
    }

    return ServerResponse.ok().body(Flux.fromIterable(slice.content), T::class.java)
}
