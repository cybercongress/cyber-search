package fund.cyber.pump.common

import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.pump.common.node.BlockchainInterface
import fund.cyber.pump.common.node.ConcurrentPulledBlockchain
import fund.cyber.pump.common.node.FlowableBlockchainInterface
import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.configuration.CHAIN_NODE_URL
import fund.cyber.search.configuration.PUMP_MAX_CONCURRENCY
import fund.cyber.search.configuration.PUMP_MAX_CONCURRENCY_DEFAULT
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.retry.RetryCallback
import org.springframework.retry.RetryContext
import org.springframework.retry.backoff.ExponentialBackOffPolicy
import org.springframework.retry.listener.RetryListenerSupport
import org.springframework.retry.policy.AlwaysRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler
import org.springframework.web.reactive.config.EnableWebFlux


private val log = LoggerFactory.getLogger(DefaultRetryListenerSupport::class.java)!!

class DefaultRetryListenerSupport : RetryListenerSupport() {

    override fun <T : Any?, E : Throwable?> onError(context: RetryContext, callback: RetryCallback<T, E>?,
                                                    throwable: Throwable) {
        if (context.retryCount == 1) log.error("Error occurred. Start retrying...", throwable)
        super.onError(context, callback, throwable)
    }
}

@EnableWebFlux
@EnableScheduling
@Configuration
class CommonConfiguration(
    @Value("\${$PUMP_MAX_CONCURRENCY:$PUMP_MAX_CONCURRENCY_DEFAULT}")
    private val maxConcurrency: Int
) {


    @Bean
    fun chainInfo(): ChainInfo {
        val chainFamilyAsString = env(CHAIN_FAMILY, "")
        val chainFamily = ChainFamily.valueOf(chainFamilyAsString)
        val chainName = env(CHAIN_NAME, chainFamily.toString())
        val chainNodeUrl = env(CHAIN_NODE_URL, chainFamily.defaultNodeUrl)

        return ChainInfo(ChainFamily.valueOf(chainFamilyAsString), chainName, chainNodeUrl)
    }

    @Bean
    fun metricsCommonTags(chainInfo: ChainInfo) = MeterRegistryCustomizer<MeterRegistry> { registry ->
        registry.config().commonTags(
            "chainName", chainInfo.name,
            "chainFamily", chainInfo.family.name
        )
    }

    @Bean
    fun taskScheduler(): TaskScheduler = ConcurrentTaskScheduler()

    @Bean
    fun retryTemplate(): RetryTemplate {
        val retryTemplate = RetryTemplate()
        retryTemplate.setBackOffPolicy(ExponentialBackOffPolicy())
        retryTemplate.setRetryPolicy(AlwaysRetryPolicy())
        retryTemplate.registerListener(DefaultRetryListenerSupport())
        return retryTemplate
    }

    @Bean
    fun <T : BlockBundle> blockchainInterface(
        blockchainInterface: BlockchainInterface<T>,
        retryTemplate: RetryTemplate
    ): FlowableBlockchainInterface<T> {
        return ConcurrentPulledBlockchain(blockchainInterface = blockchainInterface, retryTemplate = retryTemplate,
            maxConcurrency = maxConcurrency)
    }

}
