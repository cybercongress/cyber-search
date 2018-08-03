package fund.cyber.supply.bitcoin

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.common.MINUS_ONE
import fund.cyber.common.sum
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinSupply
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.supplyTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.BatchMessageListener
import java.math.BigDecimal

class CalculateBitcoinSupplyProcess(
    private val chainInfo: ChainInfo,
    private var currentSupply: BitcoinSupply,
    private val kafka: KafkaTemplate<PumpEvent, Any>,
    private val deserializer: ObjectMapper = jsonDeserializer
) : BatchMessageListener<PumpEvent, ByteArray> {

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, ByteArray>>) {
        currentSupply = calculateNewSupply(records)
        // kafka send will be executing in transaction, so no need to check, if it succeed
        kafka.send(ProducerRecord<PumpEvent, Any>(chainInfo.supplyTopic, currentSupply))
    }

    private fun calculateNewSupply(records: List<ConsumerRecord<PumpEvent, ByteArray>>): BitcoinSupply {

        val blocks = records.filter { record -> record.topic() == chainInfo.blockPumpTopic }
            .map { record -> record.key() to deserializer.readValue(record.value(), BitcoinBlock::class.java) }

        val blockNumberDelta = blocks.map { (event, _) -> if (event == PumpEvent.NEW_BLOCK) 1 else -1 }.sum()
        val miningBlocksSupplyDelta = blocks.map { (event, block) -> block.blockReward * event.sign }.sum()

        return BitcoinSupply(
            blockNumber = currentSupply.blockNumber + blockNumberDelta,
            totalSupply = currentSupply.totalSupply + miningBlocksSupplyDelta
        )
    }
}

private val PumpEvent.sign get() = if (this == PumpEvent.NEW_BLOCK) BigDecimal.ONE else MINUS_ONE