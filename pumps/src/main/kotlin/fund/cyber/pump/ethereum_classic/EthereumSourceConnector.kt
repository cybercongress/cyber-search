package fund.cyber.pump.ethereum_classic

//import fund.cyber.node.connectors.configuration.ethereumConnectorConfiguration
//import org.apache.kafka.common.config.ConfigDef
//import org.apache.kafka.connect.connector.Task
//import org.apache.kafka.connect.source.SourceConnector



class EthereumSourceConnector {

    private lateinit var properties: Map<String, String>

    fun start(properties: Map<String, String>) {
        this.properties = properties
    }

    fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        return listOf(properties)
    }
    fun version(): String = "1.0"
//    fun taskClass(): Class<out Task> = EthereumSourceConnectorTask::class.java
//    fun config(): ConfigDef = ethereumConnectorConfiguration
    fun stop() {}
}