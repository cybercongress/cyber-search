package fund.cyber.supply

import fund.cyber.common.kafka.JsonSerializer
import fund.cyber.common.kafka.defaultProducerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import fund.cyber.search.model.events.supplyTopic
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.transaction.KafkaTransactionManager
import javax.annotation.PostConstruct


@Configuration
@ComponentScan("fund.cyber.supply")
class CommonConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Value("\${$CHAIN_FAMILY:}")
    private lateinit var chainFamily: String

    @Value("\${$CHAIN_NAME:}")
    private lateinit var chainName: String


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


}