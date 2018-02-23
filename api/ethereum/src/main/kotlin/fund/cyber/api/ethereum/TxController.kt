package fund.cyber.api.ethereum

import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono


@RestController
@RequestMapping("/ethereum")
class TxController(
        private val txRepository: EthereumTxRepository
) {

    @GetMapping("/tx/{hash}")
    fun txByHash(@PathVariable hash: String): Mono<CqlEthereumTx> {
        return txRepository.findById(hash)
    }
}