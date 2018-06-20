package fund.cyber.contract.bitcoin.delta.apply

import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.contract.bitcoin.BitcoinContractSummaryStorage
import fund.cyber.contract.bitcoin.summary.BitcoinDeltaMerger
import fund.cyber.contract.bitcoin.summary.BitcoinTxDeltaProcessor
import fund.cyber.contract.common.delta.apply.UpdateContractSummaryProcess
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.BATCH
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties
import org.springframework.transaction.annotation.EnableTransactionManagement
import javax.annotation.Resource


@EnableKafka
@Configuration
@EnableTransactionManagement
class BitcoinTxConsumerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chainInfo: ChainInfo

    @Autowired
    private lateinit var txDeltaProcessor: BitcoinTxDeltaProcessor

    @Autowired
    private lateinit var contractSummaryStorage: BitcoinContractSummaryStorage

    @Autowired
    private lateinit var deltaMerger: BitcoinDeltaMerger

    @Autowired
    private lateinit var monitoring: MeterRegistry

    @Resource(name = "consumerConfigs")
    private lateinit var consumerConfigs: Map<String, Any>

    @Bean
    fun txListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, BitcoinTx> {

        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerConfigs, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(BitcoinTx::class.java)
        )

        val containerProperties = ContainerProperties(chainInfo.txPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdateContractSummaryProcess(
                contractSummaryStorage, txDeltaProcessor, deltaMerger, monitoring, kafkaBrokers
            )
            isAckOnError = false
            ackMode = BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }
}
