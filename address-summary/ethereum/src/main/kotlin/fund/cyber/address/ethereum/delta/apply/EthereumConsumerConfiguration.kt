package fund.cyber.address.ethereum.delta.apply

import fund.cyber.address.common.delta.apply.UpdatesAddressSummaryProcess
import fund.cyber.address.ethereum.EthereumAddressSummaryStorage
import fund.cyber.address.ethereum.summary.EthereumBlockDeltaProcessor
import fund.cyber.address.ethereum.summary.EthereumDeltaMerger
import fund.cyber.address.ethereum.summary.EthereumTxDeltaProcessor
import fund.cyber.address.ethereum.summary.EthereumUncleDeltaProcessor
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties
import org.springframework.transaction.annotation.EnableTransactionManagement

@EnableKafka
@Configuration
@EnableTransactionManagement
class EthereumTxConsumerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chain: Chain

    @Autowired
    private lateinit var txDeltaProcessor: EthereumTxDeltaProcessor

    @Autowired
    private lateinit var blockDeltaProcessor: EthereumBlockDeltaProcessor

    @Autowired
    private lateinit var uncleDeltaProcessor: EthereumUncleDeltaProcessor

    @Autowired
    private lateinit var addressSummaryStorage: EthereumAddressSummaryStorage

    @Autowired
    private lateinit var deltaMerger: EthereumDeltaMerger

    @Autowired
    private lateinit var monitoring: MeterRegistry

    @Bean
    fun txListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, EthereumTx> {

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumTx::class.java)
        )

        val containerProperties = ContainerProperties(chain.txPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdatesAddressSummaryProcess(addressSummaryStorage, txDeltaProcessor, deltaMerger, monitoring)
            isAckOnError = false
            ackMode = AbstractMessageListenerContainer.AckMode.BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }

    @Bean
    fun blockListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, EthereumBlock> {

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumBlock::class.java)
        )

        val containerProperties = ContainerProperties(chain.blockPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdatesAddressSummaryProcess(addressSummaryStorage, blockDeltaProcessor, deltaMerger, monitoring)
            isAckOnError = false
            ackMode = AbstractMessageListenerContainer.AckMode.BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }

    @Bean
    fun uncleListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, EthereumUncle> {

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumUncle::class.java)
        )

        val containerProperties = ContainerProperties(chain.unclePumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdatesAddressSummaryProcess(addressSummaryStorage, uncleDeltaProcessor, deltaMerger, monitoring)
            isAckOnError = false
            ackMode = AbstractMessageListenerContainer.AckMode.BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }

    private fun consumerConfigs(): MutableMap<String, Any> = mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.GROUP_ID_CONFIG to "ethereum-address-summary-update-process",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 10
    )
}