package fund.cyber.cassandra

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.extras.codecs.jdk8.InstantCodec
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*


class CassandraService(
        private val cassandraServers: List<String>,
        private val cassandraPort: Int
) {

    val cassandra by lazy {
        Cluster.builder()
                .addContactPoints(*cassandraServers.toTypedArray())
                .withPort(cassandraPort)
                .withMaxSchemaAgreementWaitSeconds(1)
                .build().init()!!
                .apply { configuration.codecRegistry.register(InstantCodec.instance) }
    }

    val bitcoinRepositorySession: Session by lazy { cassandra.connect(BITCOIN.lowercaseName()) }
    val bitcoinCashRepositorySession: Session by lazy { cassandra.connect(BITCOIN_CASH.lowercaseName()) }
    val ethereumRepositorySession: Session by lazy { cassandra.connect(ETHEREUM.lowercaseName()) }
    val ethereumClassicRepositorySession: Session by lazy { cassandra.connect(ETHEREUM_CLASSIC.lowercaseName()) }

    val bitcoinRepository by lazy { MappingManager(bitcoinRepositorySession) }
    val bitcoinCashRepository by lazy { MappingManager(bitcoinCashRepositorySession) }
    val ethereumRepository by lazy { MappingManager(ethereumRepositorySession) }
    val ethereumClassicRepository by lazy { MappingManager(ethereumClassicRepositorySession) }

    val systemKeyspaceRepository by lazy { CyberSystemKeyspaceRepository(cassandra) }
    val pumpKeyspaceRepository by lazy { PumpsKeyspaceRepository(cassandra) }


    fun getChainRepository(chain: Chain): MappingManager {
        return when (chain) {
            BITCOIN -> bitcoinRepository
            BITCOIN_CASH -> bitcoinCashRepository
            ETHEREUM -> ethereumRepository
            ETHEREUM_CLASSIC -> ethereumClassicRepository
        }
    }

    fun getChainRepositorySession(chain: Chain): Session {
        return when (chain) {
            BITCOIN -> bitcoinRepositorySession
            BITCOIN_CASH -> bitcoinCashRepositorySession
            ETHEREUM -> ethereumRepositorySession
            ETHEREUM_CLASSIC -> ethereumClassicRepositorySession
        }
    }

    fun close() = cassandra.closeAsync()
}