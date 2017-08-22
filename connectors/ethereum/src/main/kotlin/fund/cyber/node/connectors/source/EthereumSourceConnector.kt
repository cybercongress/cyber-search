package fund.cyber.node.connectors.source

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import java.util.HashMap
import java.util.ArrayList



class EthereumSourceConnector : SourceConnector() {

    override fun version(): String {
        return "1.0"
    }

    override fun taskClass(): Class<out Task> {
        return EthereumSourceConnectorTask::class.java
    }

    override fun config(): ConfigDef {
        return ConfigDef()
    }

    override fun stop() {
    }

    override fun start(props: MutableMap<String, String>?) {
    }

    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {

        val configs = ArrayList<Map<String, String>>()
        val config = HashMap<String, String>()
        configs.add(config)
        return configs
    }
}