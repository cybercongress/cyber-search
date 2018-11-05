package fund.cyber.common.kafka

import kafka.common.KafkaException
import org.springframework.kafka.test.rule.KafkaEmbedded

object KafkaEmbeddedHolder {
    val embeddedKafka = KafkaEmbedded(1, false)
    var started = false

    init {
        Runtime.getRuntime().addShutdownHook(Thread(Runnable(function = {
            embeddedKafka.destroy()
        })))
    }

    fun getKafka(): KafkaEmbedded {
        if (!started) {
            try {
                embeddedKafka.brokerProperty("log.dirs", "./kafka-logs/")
                embeddedKafka.brokerProperty("auto.create.topics.enable", "true")
                embeddedKafka.brokerProperty("transaction.state.log.replication.factor", "1")
                embeddedKafka.brokerProperty("transaction.state.log.min.isr", "1")
                embeddedKafka.before()
            } catch (e: Exception) {
                throw KafkaException(e)
            }
            started = true
        }
        return embeddedKafka
    }
}
