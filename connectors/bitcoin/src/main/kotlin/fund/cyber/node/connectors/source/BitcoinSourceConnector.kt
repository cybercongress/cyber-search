package fund.cyber.node.connectors.source

import fund.cyber.node.connectors.configuration.bitcoinConnectorConfiguration
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector



val bitcoin = "bitcoin"

class BitcoinSourceConnector : SourceConnector() {

    private lateinit var properties: Map<String, String>

    override fun start(properties: Map<String, String>) {
        this.properties = properties
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        return listOf(properties)
    }
    override fun version(): String = "1.0"
    override fun taskClass(): Class<out Task> = BitcoinSourceConnectorTask::class.java
    override fun config(): ConfigDef = bitcoinConnectorConfiguration
    override fun stop() {}
}