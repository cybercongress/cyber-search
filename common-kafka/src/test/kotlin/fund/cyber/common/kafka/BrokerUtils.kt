package fund.cyber.common.kafka

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type.TOPIC
import java.util.*


fun AdminClient.getTopicConfig(topic: String): Config? {
    val topicResource = ConfigResource(TOPIC, topic)
    val configs = this.describeConfigs(listOf(topicResource))
    return configs.all().get()[topicResource]
}

fun adminClientProperties(kafkaBrokers: String) = Properties().apply {
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers)
}
