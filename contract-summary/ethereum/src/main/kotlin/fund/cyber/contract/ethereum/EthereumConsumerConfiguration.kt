package fund.cyber.contract.ethereum

import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.contract.common.delta.apply.UpdateContractSummaryProcess
import fund.cyber.contract.ethereum.delta.EthereumBlockDeltaProcessor
import fund.cyber.contract.ethereum.delta.EthereumDeltaMerger
import fund.cyber.contract.ethereum.delta.EthereumTxDeltaProcessor
import fund.cyber.contract.ethereum.delta.EthereumUncleDeltaProcessor
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
import io.micrometer.core.instrument.MeterRegistry
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
import javax.annotation.Resource

@EnableKafka
@Configuration
@EnableTransactionManagement
class EthereumTxConsumerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var txDeltaProcessor: EthereumTxDeltaProcessor

    @Autowired
    private lateinit var blockDeltaProcessor: EthereumBlockDeltaProcessor

    @Autowired
    private lateinit var uncleDeltaProcessor: EthereumUncleDeltaProcessor

    @Autowired
    private lateinit var contractSummaryStorage: EthereumContractSummaryStorage

    @Autowired
    private lateinit var deltaMerger: EthereumDeltaMerger

    @Autowired
    private lateinit var monitoring: MeterRegistry

    @Autowired
    private lateinit var chainInfo: ChainInfo

    @Resource(name = "consumerConfigs")
    private lateinit var consumerConfigs: Map<String, Any>

    @Bean
    fun txListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, EthereumTx> {

        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerConfigs, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumTx::class.java)
        )

        val containerProperties = ContainerProperties(chainInfo.txPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdateContractSummaryProcess(contractSummaryStorage, txDeltaProcessor, deltaMerger,
                monitoring, kafkaBrokers)
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
            consumerConfigs, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumBlock::class.java)
        )

        val containerProperties = ContainerProperties(chainInfo.blockPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdateContractSummaryProcess(contractSummaryStorage, blockDeltaProcessor, deltaMerger,
                monitoring, kafkaBrokers)
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
            consumerConfigs, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumUncle::class.java)
        )

        val containerProperties = ContainerProperties(chainInfo.unclePumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdateContractSummaryProcess(contractSummaryStorage, uncleDeltaProcessor, deltaMerger,
                monitoring, kafkaBrokers)
            isAckOnError = false
            ackMode = AbstractMessageListenerContainer.AckMode.BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }
}
