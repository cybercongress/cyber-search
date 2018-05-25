package fund.cyber.supply.ethereum

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.common.MINUS_ONE
import fund.cyber.common.sum
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumSupply
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.PumpEvent.NEW_BLOCK
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.supplyTopic
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.BatchMessageListener
import java.math.BigDecimal.ONE

class CalculateEthereumSupplyProcess(
    private val chainInfo: ChainInfo,
    private var currentSupply: EthereumSupply,
    private val kafka: KafkaTemplate<PumpEvent, Any>,
    private val deserializer: ObjectMapper = jsonDeserializer
) : BatchMessageListener<PumpEvent, ByteArray> {

    override fun onMessage(records: List<ConsumerRecord<PumpEvent, ByteArray>>) {
        currentSupply = calculateNewSupply(records)
        // kafka send will be executing in transaction, so no need to check, if it succeed
        kafka.send(ProducerRecord<PumpEvent, Any>(chainInfo.supplyTopic, currentSupply))
    }

    private fun calculateNewSupply(records: List<ConsumerRecord<PumpEvent, ByteArray>>): EthereumSupply {

        val blocks = records.filter { record -> record.topic() == chainInfo.blockPumpTopic }
            .map { record -> record.key() to deserializer.readValue(record.value(), EthereumBlock::class.java) }

        val uncles = records.filter { record -> record.topic() == chainInfo.unclePumpTopic }
            .map { record -> record.key() to deserializer.readValue(record.value(), EthereumUncle::class.java) }

        val blockNumberDelta = blocks.map { (event, _) -> if (event == NEW_BLOCK) 1 else -1 }.sum()
        val miningBlocksSupplyDelta = blocks.map { (event, block) -> block.blockReward * event.sign }.sum()
        val includingUnclesSupplyDelta = blocks.map { (event, block) -> block.unclesReward * event.sign }.sum()

        val unclesNumberDelta = uncles.map { (event, _) -> if (event == NEW_BLOCK) 1 else -1 }.sum()
        val miningUnclesSupplyDelta = uncles.map { (event, uncle) -> uncle.uncleReward * event.sign }.sum()
        val totalSupplyDelta = miningBlocksSupplyDelta + miningUnclesSupplyDelta + includingUnclesSupplyDelta

        return EthereumSupply(
            blockNumber = currentSupply.blockNumber + blockNumberDelta,
            uncleNumber = currentSupply.uncleNumber + unclesNumberDelta,
            totalSupply = currentSupply.totalSupply + totalSupplyDelta,
            genesisSupply = currentSupply.genesisSupply,
            miningBlocksSupply = currentSupply.miningBlocksSupply + miningBlocksSupplyDelta,
            miningUnclesSupply = currentSupply.miningUnclesSupply + miningUnclesSupplyDelta,
            includingUnclesSupply = currentSupply.includingUnclesSupply + includingUnclesSupplyDelta
        )
    }
}

private val PumpEvent.sign get() = if (this == NEW_BLOCK) ONE else MINUS_ONE