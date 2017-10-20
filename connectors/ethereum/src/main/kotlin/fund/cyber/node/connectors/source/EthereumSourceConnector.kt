package fund.cyber.node.connectors.source

import fund.cyber.node.connectors.configuration.ethereumConnectorConfiguration
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector



class EthereumSourceConnector : SourceConnector() {

    private lateinit var properties: Map<String, String>

    override fun start(properties: Map<String, String>) {
        this.properties = properties
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        return listOf(properties)
    }
    override fun version(): String = "1.0"
    override fun taskClass(): Class<out Task> = EthereumSourceConnectorTask::class.java
    override fun config(): ConfigDef = ethereumConnectorConfiguration
    override fun stop() {}
}