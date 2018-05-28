package fund.cyber.supply.ethereum

import fund.cyber.common.kafka.DEFAULT_POLL_TIMEOUT
import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.common.kafka.JsonSerializer
import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.kafka.defaultProducerConfig
import fund.cyber.common.kafka.reader.SinglePartitionTopicLastItemsReader
import fund.cyber.common.with
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.configuration.GENESIS_SUPPLY
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumSupply
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.blockPumpTopic
import fund.cyber.search.model.events.supplyTopic
import fund.cyber.search.model.events.unclePumpTopic
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode.BATCH
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties
import org.springframework.kafka.transaction.KafkaTransactionManager
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import javax.annotation.PostConstruct


private val log = LoggerFactory.getLogger(ApplicationConfiguration::class.java)!!

@Configuration
class ApplicationConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Value("\${$CHAIN_FAMILY:}")
    private lateinit var chainFamily: String

    @Value("\${$CHAIN_NAME:}")
    private lateinit var chainName: String

    @Value("\${$GENESIS_SUPPLY:}")
    private lateinit var genesisSupply: String

    @Bean
    fun chainInfo() = ChainInfo(ChainFamily.valueOf(chainFamily), chainName)

    @Bean
    fun producerFactory(): ProducerFactory<PumpEvent, Any> {

        val config = defaultProducerConfig().with(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers
        )
        return DefaultKafkaProducerFactory<PumpEvent, Any>(config, JsonSerializer(), JsonSerializer())
            .apply { setTransactionIdPrefix(chainInfo().fullName + "_SUPPLY") }
    }


    @Bean
    fun transactionManager() = KafkaTransactionManager(producerFactory())

    @Bean
    fun kafkaTemplate() = KafkaTemplate(producerFactory())


    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers)
        return KafkaAdmin(configs).apply { this.setFatalIfBrokerNotAvailable(true) }
    }

    @PostConstruct
    fun createTopics() {

        val kafkaClient = AdminClient.create(kafkaAdmin().config)

        val supplyTopicConfig = mapOf(
            TopicConfig.RETENTION_BYTES_CONFIG to "52428800",
            TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_DELETE
        )

        val supplyTopic = NewTopic(chainInfo().supplyTopic, 1, 1).configs(supplyTopicConfig)

        kafkaClient.createTopics(listOf(supplyTopic))
        kafkaClient.close()
    }

    @Bean
    fun blocksAndUnclesListenerContainerFactory(): KafkaMessageListenerContainer<PumpEvent, ByteArray> {

        val consumerConfig = consumerConfigs().apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "ethereum-supply-process")
        }

        val consumerFactory = DefaultKafkaConsumerFactory(
            consumerConfig, JsonDeserializer(PumpEvent::class.java), ByteArrayDeserializer()
        )

        val containerProperties = ContainerProperties(chainInfo().blockPumpTopic, chainInfo().unclePumpTopic).apply {
            messageListener = CalculateEthereumSupplyProcess(
                chainInfo = chainInfo(), currentSupply = getLastCalculatedSupply(), kafka = kafkaTemplate()
            )
            pollTimeout = DEFAULT_POLL_TIMEOUT
            ackMode = BATCH
            transactionManager = transactionManager()
            setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
        }

        return KafkaMessageListenerContainer(consumerFactory, containerProperties)
    }

    private fun getLastCalculatedSupply(): EthereumSupply {

        val topicReader = SinglePartitionTopicLastItemsReader(
            kafkaBrokers = kafkaBrokers, topic = chainInfo().supplyTopic,
            keyClass = Any::class.java, valueClass = EthereumSupply::class.java
        )

        val keyToValue = topicReader.readLastRecords(1).firstOrNull()
        return keyToValue?.second ?: genesisSupply()
    }

    private fun genesisSupply(): EthereumSupply {

        if (genesisSupply.trim().isEmpty()) {
            log.error("Please specify env variable `GENESIS_SUPPLY`. " +
                "For example, initial Ethereum supply is 72009990.50")
            throw RuntimeException("`GENESIS_SUPPLY` is not provided")
        }

        return EthereumSupply(
            blockNumber = 0, uncleNumber = 0,
            totalSupply = BigDecimal(genesisSupply), genesisSupply = BigDecimal(genesisSupply),
            miningBlocksSupply = ZERO, miningUnclesSupply = ZERO, includingUnclesSupply = ZERO
        )
    }

    private fun consumerConfigs(): MutableMap<String, Any> = defaultConsumerConfig().with(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase()
    )
}