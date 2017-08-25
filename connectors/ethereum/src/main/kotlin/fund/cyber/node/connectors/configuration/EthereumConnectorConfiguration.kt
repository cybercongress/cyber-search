package fund.cyber.node.connectors.configuration

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance.HIGH
import org.apache.kafka.common.config.ConfigDef.Type.*


val PARITY_URL = "parity.url"
val CHUNK_SIZE = "chunk.size"
val BATCH_SIZE = "batch.size"

val batch_size_default = 32

val ethereumConnectorConfiguration = ConfigDef()
        .define(PARITY_URL, STRING, "http://127.0.0.1:8545", HIGH, "Define parity url.")!!
        .define(CHUNK_SIZE, LONG, 500, HIGH, "Define cassandra row key granularity.")!!
        .define(BATCH_SIZE, INT, batch_size_default, HIGH, "Define number of concurrent requests to parity.")!!


class EthereumConnectorConfiguration(
        properties: Map<String, String>
) : AbstractConfig(ethereumConnectorConfiguration, properties, true) {

    val parityUrl = getString(PARITY_URL)!!
    val chunkSize = getLong(CHUNK_SIZE)!!
    val batchSize = getInt(BATCH_SIZE)!!
}