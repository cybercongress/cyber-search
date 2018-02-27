package fund.cyber.pump.ethereum

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.common.ChainEntity.UNCLE
import fund.cyber.node.kafka.ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX
import fund.cyber.node.kafka.PumpEvent
import fund.cyber.node.kafka.PumpEvent.DROPPED_BLOCK
import fund.cyber.node.kafka.PumpEvent.NEW_BLOCK
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.EthereumItem
import fund.cyber.pump.kafka.KafkaStorageActionTemplate
import fund.cyber.pump.kafka.KafkaStorageActionTemplateFactory
import org.apache.kafka.clients.producer.ProducerRecord


class EthereumKafkaStorageActionTemplateFactory(
        chain: Chain
) : KafkaStorageActionTemplateFactory<EthereumBlockBundle> {

    private val transactionsTopic = chain.entityTopic(TRANSACTION)
    private val unclesTopic = chain.entityTopic(UNCLE)
    private val addressBlocksTopic = chain.name + ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX

    override fun constructActionTemplate(bundle: EthereumBlockBundle): KafkaStorageActionTemplate {

        val newBlockTxesRecords = bundle.transactions.map { tx -> asRecord(transactionsTopic, NEW_BLOCK, tx) }
        val newBlockUnclesRecords = bundle.uncles.map { uncle -> asRecord(unclesTopic, NEW_BLOCK, uncle) }
        val newAddressBlockRecord = asRecord(addressBlocksTopic, NEW_BLOCK, bundle.addressBlock)
        val storeRecords = newBlockTxesRecords + newBlockUnclesRecords + newAddressBlockRecord

        val dropBlockTxesRecords = bundle.transactions.map { tx -> asRecord(transactionsTopic, DROPPED_BLOCK, tx) }
        val dropBlockUnclesRecords = bundle.uncles.map { uncle -> asRecord(unclesTopic, DROPPED_BLOCK, uncle) }
        val dropAddressBlockRecord = asRecord(addressBlocksTopic, DROPPED_BLOCK, bundle.addressBlock)
        val dropRecords = dropBlockTxesRecords + dropBlockUnclesRecords + dropAddressBlockRecord

        return KafkaStorageActionTemplate(storeRecords, dropRecords)
    }

    private fun asRecord(topic: String, event: PumpEvent, tx: EthereumItem): ProducerRecord<PumpEvent, EthereumItem>
            = ProducerRecord(topic, event, tx)
}