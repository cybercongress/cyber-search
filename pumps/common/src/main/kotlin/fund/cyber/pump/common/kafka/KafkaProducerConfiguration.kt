package fund.cyber.pump.common.kafka

import fund.cyber.common.kafka.JsonSerializer
import fund.cyber.common.kafka.defaultProducerConfig
import fund.cyber.common.kafka.idempotentProducerDefaultConfig
import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.common.with
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.TopicConfig
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import java.util.concurrent.TimeUnit
import javax.annotation.PostConstruct

private const val CLEANUP_RETENTION_POLICY_TIME_DAYS = 365L * 1000L
private const val TOPIC_REPLICATION_FACTOR: Short = 3

@EnableKafka
@Configuration
class KafkaProducerConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chainInfo: ChainInfo

    @Bean
    fun kafkaAdmin(): KafkaAdmin {
        val configs = mapOf(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers
        )
        return KafkaAdmin(configs)
    }

    @Bean
    fun producerFactory(): ProducerFactory<PumpEvent, Any> {

        val configs = idempotentProducerDefaultConfig().with(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers)
        return DefaultKafkaProducerFactory<PumpEvent, Any>(configs, JsonSerializer(), JsonSerializer())
            .apply { setTransactionIdPrefix(chainInfo.name + "_PUMP") }
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<PumpEvent, Any> {
        return KafkaTemplate(producerFactory())
    }

    @Bean
    fun producerFactoryPool(): ProducerFactory<PumpEvent, Any> {

        val configs = defaultProducerConfig().with(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers)
        return DefaultKafkaProducerFactory<PumpEvent, Any>(configs, JsonSerializer(), JsonSerializer())
    }

    @Bean
    fun kafkaTemplatePool(): KafkaTemplate<PumpEvent, Any> {
        return KafkaTemplate(producerFactoryPool())
    }

    @Bean
    fun topicConfigs(): Map<String, String> {
        return mapOf(
            TopicConfig.RETENTION_MS_CONFIG to TimeUnit.DAYS.toMillis(CLEANUP_RETENTION_POLICY_TIME_DAYS)
                .toString(),
            TopicConfig.CLEANUP_POLICY_CONFIG to TopicConfig.CLEANUP_POLICY_DELETE,
            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG to "false"
        )
    }

    @PostConstruct
    fun createTopics() {
        val kafkaClient = AdminClient.create(kafkaAdmin().config)

        val newTopics = mutableListOf<NewTopic>()
        chainInfo.family.entityTypes.keys.forEach { entity ->
            newTopics
                .add(NewTopic(entity.kafkaTopicName(chainInfo), 1, TOPIC_REPLICATION_FACTOR).configs(topicConfigs()))
        }

        kafkaClient.createTopics(newTopics)
        kafkaClient.close()
    }
}
