package fund.cyber.api.common

import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.http.HttpStatus.FORBIDDEN
import org.springframework.web.reactive.function.BodyInserters.fromObject
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

const val MAX_PAGE_SIZE = 100
const val MIN_PAGE_SIZE = 1
val pageSizeValidationErrorMessageBody = fromObject("pageSize parameter should be in range [1,100]")


inline fun <reified T> ServerRequest.toPageableResponse(
    getPage: (Pageable) -> Slice<T>
): Mono<ServerResponse> {

    return toPageableResponse(getPage = getPage, mapPage = { pageFlux -> pageFlux })
}


inline fun <reified T, reified R> ServerRequest.toPageableResponse(
    getPage: (Pageable) -> Slice<T>, mapPage: (Flux<T>) -> Flux<R>
): Mono<ServerResponse> {

    val page = this.queryParam("page").orElse("0").toInt()
    val pageSize = this.queryParam("pageSize").orElse("20").toInt()

    if (pageSize > MAX_PAGE_SIZE || pageSize < MIN_PAGE_SIZE) {
        return ServerResponse.status(FORBIDDEN).body(pageSizeValidationErrorMessageBody)
    }

    var slice = getPage(CassandraPageRequest.first(pageSize))

    for (i in 1..page) {
        if (slice.hasNext()) {
            slice = getPage(slice.nextPageable())
        } else return ServerResponse.notFound().build()
    }
    val pageFlux = Flux.fromIterable(slice.content)
    return mapPage(pageFlux).asServerResponse()
}
