package fund.cyber.supply.bitcoin

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinSupply
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.supplyTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant

@DisplayName("Application context start-up configuration test")
class CalculateBitcoinSupplyProcessTest {

    private val chainInfo = ChainInfo(ChainFamily.BITCOIN, "")
    private val kafka = mock<KafkaTemplate<PumpEvent, Any>> {}

    private val currentSupply = BitcoinSupply(
        blockNumber = 666, totalSupply = BigDecimal("1000")
    )

    private val expectedSupply = BitcoinSupply(
        blockNumber = 668, totalSupply = BigDecimal("1052")
    )

    @Test
    @DisplayName("Should correctly create first supply entry")
    fun shouldCorrectlyCreateSupplyTopic() {

        val calculateSupplyProcess = CalculateBitcoinSupplyProcess(chainInfo, currentSupply, kafka)

        val blockRecords = listOf(
            PumpEvent.NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("11"))),
            PumpEvent.DROPPED_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("11"))),
            PumpEvent.NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("21"))),
            PumpEvent.NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("31")))
        ).map { (event, block) -> ConsumerRecord(chainInfo.blockPumpTopic, 0, 0, event, block) }

        val records = (blockRecords).shuffled()

        calculateSupplyProcess.onMessage(records)

        verify(kafka).send(ProducerRecord<PumpEvent, Any>(chainInfo.supplyTopic, expectedSupply))
    }

    private fun createBlock(blockReward: BigDecimal) = BitcoinBlock (
        height = 1, hash = "H", parentHash = "PH", minerContractHash = "miner1",
        blockReward = blockReward, txFees = BigDecimal(0), coinbaseData = "test",
        time = Instant.now(), nonce = 1, merkleroot = "1", size = 1, version = 1, weight = 1,
        bits = "1", difficulty = BigInteger("0"), txNumber = 1, totalOutputsAmount = BigDecimal("1")
    )
}
