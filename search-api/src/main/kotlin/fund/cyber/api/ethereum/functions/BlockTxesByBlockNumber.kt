package fund.cyber.api.ethereum.functions

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux

class BlockTxesByBlockNumber(
        private val repository: PageableEthereumBlockTxRepository
) : HandlerFunction<ServerResponse> {


    override fun handle(request: ServerRequest): Mono<ServerResponse> {

        val blockNumber = request.pathVariable("blockNumber").toLong()
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = repository.findAllByBlockNumber(blockNumber, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = repository.findAllByBlockNumber(blockNumber, slice.nextPageable())
            } else return ServerResponse.ok().build()
        }
        return ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumBlockTxPreview::class.java)
    }
}