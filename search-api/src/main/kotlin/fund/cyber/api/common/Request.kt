package fund.cyber.api.common

import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.web.reactive.function.server.ServerRequest
import reactor.core.publisher.Flux

inline fun <reified T> ServerRequest.toPageableFlux(getPage: (Pageable) -> Slice<T>): Flux<T> {

    val page = this.queryParam("page").orElse("0").toInt()
    val pageSize = this.queryParam("pageSize").orElse("20").toInt()

    var slice = getPage(CassandraPageRequest.first(pageSize))

    for (i in 1..page) {
        if (slice.hasNext()) {
            slice = getPage(slice.nextPageable())
        } else return Flux.empty()
    }

    return Flux.fromIterable(slice.content)
}
