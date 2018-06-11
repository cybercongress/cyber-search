package fund.cyber.dump.common

import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.kafka.kafkaTopicName
import fund.cyber.common.with
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.GenericApplicationContext
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.KafkaMessageListenerContainer
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.kafka.listener.config.ContainerProperties
import javax.annotation.PostConstruct

private const val POLL_TIMEOUT = 5000L
private const val AUTO_COMMIT_INTERVAL_MS_CONFIG = 10 * 1000

@Configuration
class CommonConfiguration(
    @Value("\${$KAFKA_LISTENER_MAX_POLL_RECORDS:$KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT}")
    private val maxPollRecords: Int,
    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private val kafkaBrokers: String,
    @Value("\${$CHAIN_FAMILY:}")
    private val chainFamily: String,
    @Value("\${$CHAIN_NAME:}")
    private val chainName: String
) : InitializingBean {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Bean
    fun chainInfo() = ChainInfo(
        ChainFamily.valueOf(chainFamily),
        if (chainName.isEmpty()) chainFamily else chainName
    )

    @Bean
    fun metricsCommonTags(): MeterRegistryCustomizer<MeterRegistry> {
        return MeterRegistryCustomizer { registry -> registry.config().commonTags("chain", chainInfo().name) }
    }

    @PostConstruct
    fun initSystemProperties() {
        System.setProperty("reactor.bufferSize.small", "8192")
    }

    private fun consumerConfigs(): MutableMap<String, Any> = defaultConsumerConfig().with(
        ConsumerConfig.GROUP_ID_CONFIG to "dump-process",
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to true,
        ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG to AUTO_COMMIT_INTERVAL_MS_CONFIG,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords
    )

    override fun afterPropertiesSet() {

        val beanFactory = applicationContext.defaultListableBeanFactory

        val listenerResolver = applicationContext.getBean(MessageListenerResolver::class.java)

        chainInfo().entityTypes.forEach { entityType ->
            val entityClazz = chainInfo().entityClassByType(entityType)!!
            val topic = entityType.kafkaTopicName(chainInfo())

            val consumerFactory = DefaultKafkaConsumerFactory(
                consumerConfigs(), JsonDeserializer(PumpEvent::class.java), JsonDeserializer(entityClazz)
            )

            val containerProperties = ContainerProperties(topic).apply {
                messageListener = listenerResolver.getListenerByType(entityType)
                pollTimeout = POLL_TIMEOUT
                setBatchErrorHandler(SeekToCurrentBatchErrorHandler())
            }

            val listener = KafkaMessageListenerContainer(consumerFactory, containerProperties)

            beanFactory.registerSingleton(topic + "_listenerContainer", listener)
        }

    }

}
