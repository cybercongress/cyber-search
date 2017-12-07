package fund.cyber.pump.bitcoin

import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.kafka.KafkaEvent.*
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.kafka.KafkaStorageActionTemplate
import fund.cyber.pump.kafka.KafkaStorageActionTemplateFactory
import org.apache.kafka.clients.producer.ProducerRecord


//todo move topic from here to coomon place
class TxRecord(event: KafkaEvent, tx: BitcoinTransaction)
    : ProducerRecord<KafkaEvent, BitcoinTransaction>("bitcoin_tx", event, tx)


class BitcoinKafkaStorageActionTemplateFactory : KafkaStorageActionTemplateFactory<BitcoinBlockBundle> {

    override fun constructActionTemplate(bundle: BitcoinBlockBundle): KafkaStorageActionTemplate {
        val newBlockTxesRecords = bundle.transactions.map { tx -> TxRecord(NEW_BLOCK_TX, tx) }
        val droppedBlockTxesRecords = bundle.transactions.map { tx -> TxRecord(DROPPED_BLOCK_TX, tx) }
        return KafkaStorageActionTemplate(newBlockTxesRecords, droppedBlockTxesRecords)
    }
}