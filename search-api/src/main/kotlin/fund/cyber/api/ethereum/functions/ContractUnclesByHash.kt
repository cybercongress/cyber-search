package fund.cyber.api.ethereum.functions

import fund.cyber.api.common.toSearchHashFormat
import fund.cyber.cassandra.ethereum.model.CqlEthereumContractMinedUncle
import fund.cyber.cassandra.ethereum.repository.PageableEthereumContractMinedUncleRepository
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux

class ContractUnclesByHash(
        private val contractMinedUncleRepository: PageableEthereumContractMinedUncleRepository
) : HandlerFunction<ServerResponse> {


    override fun handle(request: ServerRequest): Mono<ServerResponse> {

        val hash = request.pathVariable("hash")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = contractMinedUncleRepository
                .findAllByMinerContractHash(hash.toSearchHashFormat(), CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = contractMinedUncleRepository.findAllByMinerContractHash(hash, slice.nextPageable())
            } else return ServerResponse.notFound().build()
        }
        return ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumContractMinedUncle::class.java)
    }
}
