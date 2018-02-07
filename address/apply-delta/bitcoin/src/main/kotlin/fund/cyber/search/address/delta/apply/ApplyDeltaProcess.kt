package fund.cyber.search.address.delta.apply


import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.search.model.bitcoin.BitcoinTx
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component


@Component
class ApplyAddressSummaryDeltaProcess(
        private val bitcoinBlockTxRepository: BitcoinBlockTxRepository
) {

    @KafkaListener(topics = ["BITCOIN_TRANSACTION"])
    fun applyAddressSummaryDelta(tx: BitcoinTx) {


        println(tx.hash)
        val txMono = bitcoinBlockTxRepository.findAllByBlockNumber(4000)
        println(txMono.collectList().block())
    }
}