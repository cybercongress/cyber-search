package fund.cyber.api.common

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.reactivestreams.Publisher
import org.springframework.boot.actuate.metrics.web.reactive.server.WebFluxTags
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono
import java.util.concurrent.TimeUnit

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
class MetricsWebFilter(private val registry: MeterRegistry) : WebFilter {


    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        return chain.filter(exchange).compose { call -> filter(exchange, call) }
    }

    private fun filter(exchange: ServerWebExchange, call: Mono<Void>): Publisher<Void> {
        val start = System.nanoTime()
        return call.doAfterSuccessOrError { _, _ -> recordTime(exchange, start) }
    }

    //todo make uri not params depended
    private fun recordTime(exchange: ServerWebExchange, start: Long) {
        val uriTag = WebFluxTags.uri(exchange)
        if (uriTag.value == "/actuator/prometheus") return

        Timer.builder("http_requests_processing")
                .tags(listOf(uriTag))
                .publishPercentiles(0.95, 0.995)
                .register(registry)
                .record(System.nanoTime() - start, TimeUnit.NANOSECONDS)
    }
}
