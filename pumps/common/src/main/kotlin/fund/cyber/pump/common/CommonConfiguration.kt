package fund.cyber.pump.common

import fund.cyber.common.kafka.JsonDeserializer
import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.pump.common.node.ConcurrentPulledBlockchain
import fund.cyber.pump.common.node.FlowableBlockchainInterface
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.requests.IsolationLevel
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.retry.RetryCallback
import org.springframework.retry.RetryContext
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.listener.RetryListenerSupport
import org.springframework.retry.policy.AlwaysRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.web.reactive.config.EnableWebFlux
import java.util.*

private val log = LoggerFactory.getLogger(DefaultRetryListenerSupport::class.java)!!

class DefaultRetryListenerSupport: RetryListenerSupport() {

    override fun <T : Any?, E : Throwable?> onError(context: RetryContext, callback: RetryCallback<T, E>?, throwable: Throwable) {
        if (context.retryCount == 1) log.error("Error occurred. Start retrying...", throwable)
        super.onError(context, callback, throwable)
    }
}

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

    @Bean
    fun retryTemplate(): RetryTemplate {
        val retryTemplate = RetryTemplate()
        retryTemplate.setBackOffPolicy(ExponentialBackOffPolicy())
        retryTemplate.setRetryPolicy(AlwaysRetryPolicy())
        retryTemplate.registerListener(DefaultRetryListenerSupport())
        return retryTemplate
    }

    @Bean
    fun <T: BlockBundle> blockchainInterface(
            blockchainInterface: BlockchainInterface<T>,
            retryTemplate: RetryTemplate
    ): FlowableBlockchainInterface<T> {
        return ConcurrentPulledBlockchain(blockchainInterface = blockchainInterface, retryTemplate = retryTemplate)
    }

    private fun consumerProperties() = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
        put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase())
    }
}