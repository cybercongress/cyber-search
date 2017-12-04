package fund.cyber.search.configuration

import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.node.model.SearchRequestProcessingStats
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*


class SearchRequestProcessingStatsRecord(
        stats: SearchRequestProcessingStats
) : ProducerRecord<String, SearchRequestProcessingStats>("search_request_processing_stats", stats)


class SearchRequestProcessingStatsKafkaProducer : KafkaProducer<String, SearchRequestProcessingStats>(
        searchRequestProcessingStatsKafkaProducerProperties(),
        StringSerializer(), JsonSerializer()
)

private fun searchRequestProcessingStatsKafkaProducerProperties(): Properties {
    return Properties().apply {
        put("bootstrap.servers", SearchApiConfiguration.kafkaBrokers)
        put("group.id", "search.api")
    }
}