package fund.cyber.search.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumBlockTxRepository
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux


@RestController
@RequestMapping("/ethereum")
class BlockController(
        private val blockRepository: EthereumBlockRepository,
        private val blockTxRepository: PageableEthereumBlockTxRepository
) {

    @GetMapping("/block/{blockNumber}")
    fun blockByNumber(@PathVariable blockNumber: Long): Mono<CqlEthereumBlock> {
        return blockRepository.findById(blockNumber)
    }

    //todo slice caching, general web caching
    @GetMapping("/block/{blockNumber}/transactions")
    fun txesPreviewsByBlockNumber(
            @PathVariable blockNumber: Long,
            @RequestParam(required = false, defaultValue = "0") page: Int,
            @RequestParam(required = false, defaultValue = "10") pageSize: Int
    ): Flux<CqlEthereumBlockTxPreview> {

        var slice = blockTxRepository.findAllByBlockNumber(blockNumber, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = blockTxRepository.findAllByBlockNumber(blockNumber, slice.nextPageable())
            } else return Flux.empty()
        }
        return slice.content.toFlux()
    }
}