package fund.cyber.dump.ethereum

import fund.cyber.cassandra.ethereum.repository.*
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTransaction
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
import org.springframework.context.annotation.DependsOn
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties


@DependsOn("kotlinPropertyConfigurer")
@EnableKafka
@Configuration
class ApplicationConfiguration {

    @Value("%{$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String


    @Autowired
    lateinit var ethereumBlockRepository: EthereumBlockRepository
    @Autowired
    lateinit var ethereumAddressMinedBlockRepository: EthereumAddressMinedBlockRepository
    @Autowired
    lateinit var ethereumTxRepository: EthereumTxRepository
    @Autowired
    lateinit var ethereumBlockTxRepository: EthereumBlockTxRepository
    @Autowired
    lateinit var ethereumAddressTxRepository: EthereumAddressTxRepository
    @Autowired
    lateinit var ethereumUncleRepository: EthereumUncleRepository
    @Autowired
    lateinit var ethereumAddressUncleRepository: EthereumAddressUncleRepository


    @Bean
    fun chain(): EthereumFamilyChain {
        val chainAsString = env(CHAIN, "")
        return EthereumFamilyChain.valueOf(chainAsString)
    }

    @Bean
    fun blocksListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumBlock> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-blocks-dump-process4")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumBlock::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain().blockPumpTopic).apply {
            messageListener = BlockDumpProcess(ethereumBlockRepository, ethereumAddressMinedBlockRepository, chain())
            pollTimeout = 5000
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    @Bean
    fun txsListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumTransaction> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-txs-dump-process4")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumTransaction::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain().txPumpTopic).apply {
            messageListener = TxDumpProcess(ethereumTxRepository, ethereumBlockTxRepository, ethereumAddressTxRepository, chain())
            pollTimeout = 5000
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    @Bean
    fun unclesListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, EthereumUncle> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-uncles-dump-process4")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfig, JsonDeserializer(PumpEvent::class.java), JsonDeserializer(EthereumUncle::class.java)
        )

        //todo add to error handler exponential wait before retries
        val containerProperties = ContainerProperties(chain().unclePumpTopic).apply {
            messageListener = UncleDumpProcess(ethereumUncleRepository, ethereumAddressUncleRepository, chain())
            pollTimeout = 5000
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    private fun consumerConfigs(): MutableMap<String, Any> = mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to 10 * 1000,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase()
    )


}