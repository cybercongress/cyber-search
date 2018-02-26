package fund.cyber.pump.common

import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.web.reactive.config.EnableWebFlux
import java.util.*

@EnableWebFlux
@EnableScheduling
@Configuration
class CommonConfiguration {

    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private lateinit var kafkaBrokers: String

    @Autowired
    private lateinit var chain: Chain

    @Bean
    fun metricsCommonTags(): MeterRegistryCustomizer<MeterRegistry> {
        return MeterRegistryCustomizer { registry -> registry.config().commonTags("chain", chain.name) }
    }

    @Bean
    fun commonPumpConsumer(): Consumer<Any, Any> {
        return KafkaConsumer<Any, Any>(consumerProperties(), JsonDeserializer(Any::class.java), JsonDeserializer(Any::class.java))
    }

    private fun consumerProperties() = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase())
    }
}