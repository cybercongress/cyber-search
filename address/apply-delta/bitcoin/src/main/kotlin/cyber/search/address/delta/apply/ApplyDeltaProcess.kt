package cyber.search.address.delta.apply

import cyber.search.bitcoin.model.BitcoinTransaction
import cyber.search.bitcoin.repository.BitcoinBlockTxRepository
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component


@Component
class ApplyAddressSummaryDeltaProcess(
        private val bitcoinBlockTxRepository: BitcoinBlockTxRepository
) {

    @KafkaListener(topics = ["BITCOIN_TRANSACTION"])
    fun applyAddressSummaryDelta(tx: BitcoinTransaction) {


        println(tx.hash)
        val txMono = bitcoinBlockTxRepository.findAllByBlockNumber(4000)
        println(txMono.collectList().block())
    }
}