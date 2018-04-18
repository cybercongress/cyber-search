package fund.cyber.api.ethereum.functions

import fund.cyber.api.common.toSearchHashFormat
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.repository.PageableEthereumAddressMinedBlockRepository
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux

class AddressBlocksByAddress(
        private val addressMinedBlockRepository: PageableEthereumAddressMinedBlockRepository
) : HandlerFunction<ServerResponse> {


    override fun handle(request: ServerRequest): Mono<ServerResponse> {

        val id = request.pathVariable("id")
        val page = request.queryParam("page").orElse("0").toInt()
        val pageSize = request.queryParam("pageSize").orElse("20").toInt()


        var slice = addressMinedBlockRepository
                .findAllByMiner(id.toSearchHashFormat(), CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = addressMinedBlockRepository.findAllByMiner(id, slice.nextPageable())
            } else return ServerResponse.notFound().build()
        }
        return ServerResponse.ok().body(slice.content.toFlux(), CqlEthereumAddressMinedBlock::class.java)
    }
}
