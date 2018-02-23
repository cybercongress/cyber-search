package fund.cyber.api.common

import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.CORS_ALLOWED_ORIGINS
import fund.cyber.search.configuration.CORS_ALLOWED_ORIGINS_DEFAULT
import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer
import org.springframework.context.annotation.*
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.reactive.CorsWebFilter
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource
import org.springframework.web.util.pattern.PathPatternParser


private class ChainPropertySet : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        return context.environment.getProperty(CHAIN) != null
    }
}

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

@Configuration
class WebConfig {


    @Value("\${$CORS_ALLOWED_ORIGINS:$CORS_ALLOWED_ORIGINS_DEFAULT}")
    private lateinit var allowedOrigin: String

    @Bean
    fun corsFilter(): CorsWebFilter {

        val config = CorsConfiguration()
        config.addAllowedOrigin(allowedOrigin)
        config.addAllowedHeader("*")
        config.addAllowedMethod("*")

        val source = UrlBasedCorsConfigurationSource(PathPatternParser())
        source.registerCorsConfiguration("/**", config)

        return CorsWebFilter(source)
    }
}