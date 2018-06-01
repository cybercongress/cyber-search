package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.repository.EthereumContractMinedBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumContractUncleRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import fund.cyber.cassandra.ethereum.repository.EthereumBlockTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumTxRepository
import fund.cyber.cassandra.ethereum.repository.EthereumUncleRepository
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.txPumpTopic
import fund.cyber.search.model.events.unclePumpTopic
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
        private val chain: EthereumFamilyChain,
        @Value("\${$KAFKA_LISTENER_MAX_POLL_RECORDS:$KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT}")
        private val maxPollRecords: Int
) {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String


    @Autowired
    lateinit var ethereumBlockRepository: EthereumBlockRepository
    @Autowired
    lateinit var ethereumContractMinedBlockRepository: EthereumContractMinedBlockRepository
    @Autowired
    lateinit var ethereumTxRepository: EthereumTxRepository
    @Autowired
    lateinit var ethereumBlockTxRepository: EthereumBlockTxRepository
    @Autowired
    lateinit var ethereumContractTxRepository: EthereumContractTxRepository
    @Autowired
    lateinit var ethereumUncleRepository: EthereumUncleRepository
    @Autowired
    lateinit var ethereumContractUncleRepository: EthereumContractUncleRepository

    @Bean
    fun blocksListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumBlock> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-blocks-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumBlock::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain.blockPumpTopic).apply {
            messageListener = BlockDumpProcess(ethereumBlockRepository, ethereumContractMinedBlockRepository, chain)
            pollTimeout = POLL_TIMEOUT
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    @Bean
    fun txsListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumTx> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-txs-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumTx::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain.txPumpTopic).apply {
            messageListener = TxDumpProcess(ethereumTxRepository, ethereumBlockTxRepository,
                    ethereumContractTxRepository, chain)
            pollTimeout = POLL_TIMEOUT
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    @Bean
    fun unclesListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumUncle> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-uncles-dump-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumUncle::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain.unclePumpTopic).apply {
            messageListener = UncleDumpProcess(ethereumUncleRepository, ethereumContractUncleRepository, chain)
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
