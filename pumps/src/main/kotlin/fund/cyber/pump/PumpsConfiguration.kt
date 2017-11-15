package fund.cyber.pump

import com.datastax.driver.core.Cluster
import fund.cyber.node.common.env
import java.util.*


object AppContext {

    val streamsConfiguration = StreamConfiguration()

    val cassandra = Cluster.builder()
            .addContactPoint(streamsConfiguration.cassandraServers)
            .build().init()
            .apply {
                configuration.poolingOptions.maxQueueSize = 10 * 1024
            }
}


class StreamConfiguration(
        val cassandraServers: String = env("CASSANDRA_CONNECTION", "localhost"),
        private val applicationIdMinorVersion: String = env("APPLICATION_ID_SUFFIX", "0"),
        val processLastBlock: Long = env("PROCESS_LAST_BLOCK", -1),
        private val applicationId: String = "cyber.index.bitcoin.block.splitter.v1.$applicationIdMinorVersion"
) {
    fun streamProperties(): Properties {
        return Properties()
    }
}


