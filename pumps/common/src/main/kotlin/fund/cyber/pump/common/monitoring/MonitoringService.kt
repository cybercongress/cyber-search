package fund.cyber.pump.common.monitoring

import fund.cyber.search.model.chains.Chain
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component


@Component
class MonitoringService(
        chain: Chain,
        private val monitoring: MeterRegistry
) {

    private val labels = listOf(Tag.of("chain", chain.name))

    fun <T : Number> gauge(name: String, number: T) = monitoring.gauge(name, labels, number)!!

    fun timer(name: String) = Timer.builder(name).tags(labels).register(monitoring)

    fun summary(name: String, baseUnit: String) = DistributionSummary.builder(name)
            .tags(labels).baseUnit("bytes").register(monitoring)

}