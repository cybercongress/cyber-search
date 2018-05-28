package fund.cyber.supply.ethereum

import fund.cyber.common.kafka.DEFAULT_POLL_TIMEOUT
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumSupply
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import fund.cyber.supply.common.CurrentSupplyProvider
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.BATCH
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties
import org.springframework.kafka.transaction.KafkaTransactionManager
import java.math.BigDecimal
import java.math.BigDecimal.ZERO

const val MAX_RECORDS_BATCH_SIZE = 5000


@Configuration
class ApplicationConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    lateinit var chainInfo: ChainInfo

    @Autowired
    lateinit var kafkaTemplate: KafkaTemplate<PumpEvent, Any>

    @Autowired
    lateinit var kafkaTransactionManager: KafkaTransactionManager<PumpEvent, Any>

    @Autowired
    lateinit var currentSupplyProvider: CurrentSupplyProvider

    @Bean
    fun blocksAndUnclesListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, ByteArray> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-supply-process")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_RECORDS_BATCH_SIZE)
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerConfig, JsonDeserializer(PumpEvent::class.java), ByteArrayDeserializer()
        )

        val containerProperties = ContainerProperties(chainInfo.blockPumpTopic, chainInfo.unclePumpTopic).apply {
            messageListener = CalculateEthereumSupplyProcess(
                chainInfo = chainInfo, currentSupply = currentSupply(), kafka = kafkaTemplate
            )
            pollTimeout = DEFAULT_POLL_TIMEOUT
            ackMode = BATCH
            transactionManager = kafkaTransactionManager
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    fun consumerConfigs(): MutableMap<String, Any> = defaultConsumerConfig().with(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase()
    )

    private fun currentSupply(): EthereumSupply {

        val createGenesisSupplyFunction = { genesis: BigDecimal ->
            EthereumSupply(
                blockNumber = 0, uncleNumber = 0,
                totalSupply = genesis, genesisSupply = genesis,
                miningBlocksSupply = ZERO, miningUnclesSupply = ZERO, includingUnclesSupply = ZERO
            )
        }
        return currentSupplyProvider.getLastCalculatedSupply(
            EthereumSupply::class.java, createGenesisSupplyFunction
        )
    }
}