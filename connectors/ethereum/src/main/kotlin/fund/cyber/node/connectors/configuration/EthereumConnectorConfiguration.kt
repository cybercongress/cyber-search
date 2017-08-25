package fund.cyber.node.connectors.configuration

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Type.STRING


val PARITY_URL = "parity.url"

val ethereumConnectorConfiguration = ConfigDef()
        .define(PARITY_URL, STRING, "http://127.0.0.1:8545", HIGH, "Define parity url.")!!


class EthereumConnectorConfiguration(
        properties: Map<String, String>
) : AbstractConfig(ethereumConnectorConfiguration, properties, true) {

    val parityUrl = getString(PARITY_URL)!!
}