package fund.cyber.api.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressTxPreview
import fund.cyber.cassandra.ethereum.repository.EthereumAddressRepository
import fund.cyber.cassandra.ethereum.repository.PageableEthereumAddressTxRepository
import fund.cyber.search.model.chains.Chain
import org.springframework.data.cassandra.core.query.CassandraPageRequest
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.toFlux


@RestController
@RequestMapping("/ethereum")
class AddressController(
        private val chain: Chain,
        private val addressRepository: EthereumAddressRepository,
        private val addressTxRepository: PageableEthereumAddressTxRepository
) {

    @GetMapping("/address/{id}")
    fun addressById(@PathVariable id: String): Mono<CqlEthereumAddressSummary> {
        return addressRepository.findById(id)
    }

    @GetMapping("/address/{id}/transactions")
    fun txesPreviewsByAddressId(
            @PathVariable id: String,
            @RequestParam(required = false, defaultValue = "0") page: Int,
            @RequestParam(required = false, defaultValue = "10") pageSize: Int
    ): Flux<CqlEthereumAddressTxPreview> {

        var slice = addressTxRepository.findAllByAddress(id, CassandraPageRequest.first(pageSize))

        for (i in 1..page) {
            if (slice.hasNext()) {
                slice = addressTxRepository.findAllByAddress(id, slice.nextPageable())
            } else return Flux.empty()
        }
        return slice.content.toFlux()
    }
}