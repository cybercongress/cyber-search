package fund.cyber.pump.bitcoin

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.kafka.PumpEvent
import fund.cyber.node.kafka.PumpEvent.DROPPED_BLOCK
import fund.cyber.node.kafka.PumpEvent.NEW_BLOCK
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.pump.kafka.KafkaStorageActionTemplate
import fund.cyber.pump.kafka.KafkaStorageActionTemplateFactory
import org.apache.kafka.clients.producer.ProducerRecord


class BitcoinKafkaStorageActionTemplateFactory(
        chain: Chain
) : KafkaStorageActionTemplateFactory<BitcoinBlockBundle> {

    private val transactionsTopic = chain.entityTopic(TRANSACTION)

    override fun constructActionTemplate(bundle: BitcoinBlockBundle): KafkaStorageActionTemplate {
        val newBlockTxesRecords = bundle.transactions.map { tx -> asRecord(NEW_BLOCK, tx) }
        val droppedBlockTxesRecords = bundle.transactions.map { tx -> asRecord(DROPPED_BLOCK, tx) }
        return KafkaStorageActionTemplate(newBlockTxesRecords, droppedBlockTxesRecords)
    }

    private fun asRecord(event: PumpEvent, tx: BitcoinTransaction): ProducerRecord<PumpEvent, BitcoinTransaction>
            = ProducerRecord(transactionsTopic, event, tx)
}