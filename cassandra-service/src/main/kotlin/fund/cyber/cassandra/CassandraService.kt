package fund.cyber.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.HostDistance
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.cassandra.repository.CyberSystemKeyspaceRepository
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.cassandra.repository.PumpsKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import org.slf4j.LoggerFactory
import com.datastax.driver.core.PoolingOptions


const val MAX_CONCURRENT_REQUESTS = 8182
const val PREFERRED_CONCURRENT_REQUEST_TO_SAVE_ENTITIES_LIST = MAX_CONCURRENT_REQUESTS / 8

private val log = LoggerFactory.getLogger(CassandraService::class.java)!!

class CassandraService(
        private val cassandraServers: List<String>,
        private val cassandraPort: Int
) {

    private val poolingOptions = PoolingOptions()
            .setMaxRequestsPerConnection(HostDistance.LOCAL, MAX_CONCURRENT_REQUESTS)
            .setMaxRequestsPerConnection(HostDistance.REMOTE, MAX_CONCURRENT_REQUESTS)

    private val cassandraLazy = lazy {
        log.info("Initializing cassandra service")
        Cluster.builder()
                .addContactPoints(*cassandraServers.toTypedArray())
                .withPort(cassandraPort)
                .withPoolingOptions(poolingOptions)
                .withMaxSchemaAgreementWaitSeconds(1)
                .build().init()!!
                .apply {
                    configuration.codecRegistry.register(InstantCodec.instance)
                    log.info("Initializing cassandra service finished")
                }
    }

    private val cassandra by cassandraLazy

    val bitcoinRepository by lazy { BitcoinKeyspaceRepository(cassandra, BITCOIN) }
    val bitcoinCashRepository by lazy { BitcoinKeyspaceRepository(cassandra, BITCOIN_CASH) }
    val ethereumRepository by lazy { EthereumKeyspaceRepository(cassandra, ETHEREUM) }
    val ethereumClassicRepository by lazy { EthereumKeyspaceRepository(cassandra, ETHEREUM_CLASSIC) }

    val systemKeyspaceRepository by lazy { CyberSystemKeyspaceRepository(cassandra) }
    val pumpKeyspaceRepository by lazy { PumpsKeyspaceRepository(cassandra) }


    fun getChainRepository(chain: Chain): CassandraKeyspaceRepository {
        return when (chain) {
            BITCOIN -> bitcoinRepository
            BITCOIN_CASH -> bitcoinCashRepository
            ETHEREUM -> ethereumRepository
            ETHEREUM_CLASSIC -> ethereumClassicRepository
        }
    }

    fun newSession(keyspace: String? = null) = if (keyspace == null) cassandra.newSession()!! else cassandra.connect(keyspace)!!

    fun close() {
        if (cassandraLazy.isInitialized()) {
            cassandra.closeAsync()
        }
    }
}