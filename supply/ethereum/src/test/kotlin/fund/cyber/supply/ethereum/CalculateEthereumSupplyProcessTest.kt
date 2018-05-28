package fund.cyber.supply.ethereum

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.verify
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.model.chains.ChainFamily.ETHEREUM
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumSupply
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.PumpEvent.DROPPED_BLOCK
import fund.cyber.search.model.events.PumpEvent.NEW_BLOCK
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.supplyTopic
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


@DisplayName("Application context start-up configuration test")
class CalculateEthereumSupplyProcessTest {

    private val chainInfo = ChainInfo(ETHEREUM, "")
    private val kafka = mock<KafkaTemplate<PumpEvent, Any>> {}

    private val currentSupply = EthereumSupply(
        blockNumber = 666, uncleNumber = 42,
        totalSupply = BigDecimal("1000"), genesisSupply = BigDecimal("700"),
        miningBlocksSupply = BigDecimal("200"), miningUnclesSupply = BigDecimal("20"),
        includingUnclesSupply = BigDecimal("80")
    )

    private val expectedSupply = EthereumSupply(
        blockNumber = 668, uncleNumber = 45,
        totalSupply = BigDecimal("1200"), genesisSupply = BigDecimal("700"),
        miningBlocksSupply = BigDecimal("252"), miningUnclesSupply = BigDecimal("114"),
        includingUnclesSupply = BigDecimal("134")
    )

    @Test
    @DisplayName("Should correctly create first supply entry")
    fun shouldCorrectlyCreateSupplyTopic() {

        val calculateSupplyProcess = CalculateEthereumSupplyProcess(chainInfo, currentSupply, kafka)

        val blockRecords = listOf(
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("11"), BigDecimal("12"))),
            DROPPED_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("11"), BigDecimal("12"))),
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("21"), BigDecimal("22"))),
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createBlock(BigDecimal("31"), BigDecimal("32")))
        ).map { (event, block) -> ConsumerRecord(chainInfo.blockPumpTopic, 0, 0, event, block) }

        val unclesRecords = listOf(
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createUncle(BigDecimal("11"))),
            DROPPED_BLOCK to jsonDeserializer.writeValueAsBytes(createUncle(BigDecimal("11"))),
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createUncle(BigDecimal("22"))),
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createUncle(BigDecimal("31"))),
            NEW_BLOCK to jsonDeserializer.writeValueAsBytes(createUncle(BigDecimal("41")))
        ).map { (event, block) -> ConsumerRecord(chainInfo.unclePumpTopic, 0, 0, event, block) }

        val records = (blockRecords + unclesRecords).shuffled()

        calculateSupplyProcess.onMessage(records)

        verify(kafka).send(ProducerRecord<PumpEvent, Any>(chainInfo.supplyTopic, expectedSupply))
    }

    private fun createBlock(blockReward: BigDecimal, unclesReward: BigDecimal) = EthereumBlock(
        unclesReward = unclesReward, blockReward = blockReward,
        hash = "H", number = 4, parentHash = "PH", txNumber = 158, minerContractHash = "miner8",
        difficulty = BigInteger("0"), totalDifficulty = BigInteger("0"), size = 0,
        txFees = BigDecimal("0"), gasUsed = 0, gasLimit = 0,
        timestamp = Instant.now(), logsBloom = "22", transactionsRoot = "22", stateRoot = "22",
        sha3Uncles = "22", nonce = 1, receiptsRoot = "22", extraData = "22", uncles = emptyList()
    )

    private fun createUncle(uncleReward: BigDecimal) = EthereumUncle(
        uncleReward = uncleReward, blockNumber = 1, blockTime = Instant.now(), blockHash = "BH", hash = "H",
        miner = "M", number = 23123, position = 0, timestamp = Instant.now()
    )
}