package fund.cyber.pump.bitcoin

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.kafka.KafkaEvent
import fund.cyber.node.kafka.KafkaEvent.DROPPED_BLOCK_TX
import fund.cyber.node.kafka.KafkaEvent.NEW_BLOCK_TX
import fund.cyber.node.kafka.chainEntityKafkaTopic
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.kafka.KafkaStorageActionTemplate
import fund.cyber.pump.kafka.KafkaStorageActionTemplateFactory
import org.apache.kafka.clients.producer.ProducerRecord


class BitcoinKafkaStorageActionTemplateFactory(
        chain: Chain
) : KafkaStorageActionTemplateFactory<BitcoinBlockBundle> {

    private val transactionsTopic = chainEntityKafkaTopic(chain, TRANSACTION)

    override fun constructActionTemplate(bundle: BitcoinBlockBundle): KafkaStorageActionTemplate {
        val newBlockTxesRecords = bundle.transactions.map { tx -> asRecord(NEW_BLOCK_TX, tx) }
        val droppedBlockTxesRecords = bundle.transactions.map { tx -> asRecord(DROPPED_BLOCK_TX, tx) }
        return KafkaStorageActionTemplate(newBlockTxesRecords, droppedBlockTxesRecords)
    }

    private fun asRecord(event: KafkaEvent, tx: BitcoinTransaction): ProducerRecord<KafkaEvent, BitcoinTransaction>
            = ProducerRecord(transactionsTopic, event, tx)
}