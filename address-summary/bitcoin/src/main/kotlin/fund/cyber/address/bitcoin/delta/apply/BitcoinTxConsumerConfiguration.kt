package fund.cyber.address.bitcoin.delta.apply

import fund.cyber.address.common.delta.apply.UpdatesAddressSummaryProcess
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.address.bitcoin.BitcoinAddressSummaryStorage
import fund.cyber.address.bitcoin.summary.BitcoinDeltaMerger
import fund.cyber.address.bitcoin.summary.BitcoinTxDeltaProcessor
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.*
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.BATCH
import org.springframework.kafka.listener.config.ContainerProperties
import org.springframework.transaction.annotation.EnableTransactionManagement

@EnableKafka
@Configuration
@EnableTransactionManagement
class BitcoinTxConsumerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chain: Chain

    @Autowired
    private lateinit var txDeltaProcessor: BitcoinTxDeltaProcessor

    @Autowired
    private lateinit var addressSummaryStorage: BitcoinAddressSummaryStorage

    @Autowired
    private lateinit var deltaMerger: BitcoinDeltaMerger

    @Autowired
    private lateinit var monitoring: MeterRegistry

    @Bean
    fun txListenerContainer(): ConcurrentMessageListenerContainer<PumpEvent, BitcoinTx> {

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(PumpEvent::class.java), JsonDeserializer(BitcoinTx::class.java)
        )

        val containerProperties = ContainerProperties(chain.txPumpTopic).apply {
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            messageListener = UpdatesAddressSummaryProcess(addressSummaryStorage, txDeltaProcessor, deltaMerger, monitoring)
            isAckOnError = false
            ackMode = BATCH
        }

        return ConcurrentMessageListenerContainer(consumerFactory, containerProperties).apply {
            concurrency = 1
        }
    }

    private fun consumerConfigs(): MutableMap<String, Any> = mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.GROUP_ID_CONFIG to "bitcoin-address-summary-update-process",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 10
    )
}