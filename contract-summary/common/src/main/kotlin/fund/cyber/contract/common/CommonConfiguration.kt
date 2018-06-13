package fund.cyber.contract.common

import fund.cyber.search.configuration.CHAIN_FAMILY
import fund.cyber.search.configuration.CHAIN_NAME
import fund.cyber.search.model.chains.ChainFamily
import fund.cyber.search.model.chains.ChainInfo
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@EnableScheduling
@Configuration
class CommonConfiguration(
    @Value("\${$CHAIN_FAMILY:}")
    private val chainFamily: String,
    @Value("\${$CHAIN_NAME:}")
    private val chainName: String
) {

    @Bean
    fun chainInfo() = ChainInfo(
        ChainFamily.valueOf(chainFamily),
        if (chainName.isEmpty()) chainFamily else chainName
    )

    @Bean
    fun metricsCommonTags(): MeterRegistryCustomizer<MeterRegistry> {
        return MeterRegistryCustomizer { registry -> registry.config().commonTags("chain", chainInfo().name) }
    }
}
