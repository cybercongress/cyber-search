package fund.cyber.api.common

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.*
import org.springframework.core.type.AnnotatedTypeMetadata


@Configuration
@Conditional(ChainPropertySet::class)
class CommonConfiguration {

    @Autowired
    private lateinit var chain: Chain

    @Bean
    fun metricsCommonTags(): MeterRegistryCustomizer<MeterRegistry> {
        return MeterRegistryCustomizer { registry -> registry.config().commonTags("chain", chain.name) }
    }
}

private class ChainPropertySet : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        return context.environment.getProperty(CHAIN) != null
    }
}