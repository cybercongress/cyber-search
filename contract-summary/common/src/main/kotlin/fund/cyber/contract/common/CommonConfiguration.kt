package fund.cyber.contract.common

import fund.cyber.common.kafka.defaultConsumerConfig
import fund.cyber.common.with
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.configuration.KAFKA_BROKERS
import fund.cyber.search.configuration.KAFKA_BROKERS_DEFAULT
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS
import fund.cyber.search.configuration.KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import javax.annotation.PostConstruct

@EnableScheduling
@Configuration
class CommonConfiguration(
    @Value("\${$CHAIN_FAMILY:}")
    private val chainFamily: String,
    @Value("\${$CHAIN_NAME:}")
    private val chainName: String,
    @Value("\${$KAFKA_LISTENER_MAX_POLL_RECORDS:$KAFKA_LISTENER_MAX_POLL_RECORDS_DEFAULT}")
    private val maxPollRecords: Int,
    @Value("\${$KAFKA_BROKERS:$KAFKA_BROKERS_DEFAULT}")
    private val kafkaBrokers: String,
    private val monitoring: MeterRegistry
) {

    @Bean
    fun chainInfo() = ChainInfo(
        ChainFamily.valueOf(chainFamily),
        if (chainName.isEmpty()) chainFamily else chainName
    )

    @Bean
    fun metricsCommonTags(chainInfo: ChainInfo) = MeterRegistryCustomizer<MeterRegistry> { registry ->
        registry.config().commonTags(
            "chainName", chainInfo.name,
            "chainFamily", chainInfo.family.name
        )
    }

    @PostConstruct
    fun initializeBatchSizeMetrics() {
        monitoring.gauge("contract-summary-process-batch-size", maxPollRecords)
    }

    @Bean
    fun consumerConfigs(): Map<String, Any> = defaultConsumerConfig().with(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaBrokers,
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        ConsumerConfig.GROUP_ID_CONFIG to "contract-summary-update-process",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().toLowerCase(),
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG to maxPollRecords
    )
}
