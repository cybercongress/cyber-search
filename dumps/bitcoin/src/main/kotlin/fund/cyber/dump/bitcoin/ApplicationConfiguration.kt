package fund.cyber.dump.bitcoin

import com.datastax.driver.core.Cluster
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractMinedBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinContractTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockTxRepository
import fund.cyber.cassandra.bitcoin.repository.BitcoinTxRepository
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.chains.BitcoinFamilyChain
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties

private const val POLL_TIMEOUT = 5000L
private const val AUTO_COMMIT_INTERVAL_MS_CONFIG = 10 * 1000

@Configuration
class ApplicationConfiguration(
        private val chain: BitcoinFamilyChain,
        @Value("\${$KAFKA_LISTENER_MAX_POLL_RECORDS:$KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT}")
        private val maxPollRecords: Int
) {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String


    @Autowired
    lateinit var blockRepository: BitcoinBlockRepository
    @Autowired
    lateinit var contractMinedBlockRepository: BitcoinContractMinedBlockRepository
    @Autowired
    lateinit var txRepository: BitcoinTxRepository
    @Autowired
    lateinit var contractTxRepository: BitcoinContractTxRepository
    @Autowired
    lateinit var blockTxRepository: BitcoinBlockTxRepository
    @Autowired
    lateinit var monitoring: MeterRegistry
    @Autowired
    lateinit var cluster: Cluster

    @Bean
    fun blocksListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, BitcoinBlock> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "bitcoin-blocks-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(BitcoinBlock::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain.blockPumpTopic).apply {
            messageListener = BlockDumpProcess(blockRepository, contractMinedBlockRepository, chain)
            pollTimeout = POLL_TIMEOUT
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    @Bean
    fun txsListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, BitcoinTx> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "bitcoin-txs-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(BitcoinTx::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain.txPumpTopic).apply {
            messageListener = TxDumpProcess(txRepository, contractTxRepository, blockTxRepository, chain, monitoring,
                cluster)
            pollTimeout = POLL_TIMEOUT
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    private fun consumerConfigs(): MutableMap<String, Any> = defaultConsumerConfig().with(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to AUTO_COMMIT_INTERVAL_MS_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords
    )
}
