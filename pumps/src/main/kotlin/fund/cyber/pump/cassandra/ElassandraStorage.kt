package fund.cyber.pump.cassandra

import fund.cyber.cassandra.CassandraService
import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.dao.migration.Migratable
import fund.cyber.node.common.Chain
import fund.cyber.node.model.IndexingProgress
import fund.cyber.pump.*
import org.apache.http.client.HttpClient
import org.slf4j.LoggerFactory
import java.util.*


class ElassandraStorage(
        private val cassandraService: CassandraService,
        httpClient: HttpClient, elasticHost: String, elasticHttpPort: Int
) : StorageInterface, StateStorage {

    private val log = LoggerFactory.getLogger(ConcurrentPulledBlockchain::class.java)!!

    private val schemaMigrationEngine = ElassandraSchemaMigrationEngine(
            cassandra = cassandraService.cassandra, httpClient = httpClient,
            elasticHost = elasticHost, elasticPort = elasticHttpPort,
            defaultMigrations = PumpsMigrations.migrations
    )

    private val actionFactories = mutableMapOf<Chain, CassandraStorageActionFactory<BlockBundle>>()

    @Suppress("UNCHECKED_CAST")
    override fun registerStorageActionFactory(chain: Chain, actionFactory: StorageActionFactory) {
        if (actionFactory is CassandraStorageActionFactory<*>) {
            actionFactories.put(chain, actionFactory as CassandraStorageActionFactory<BlockBundle>)
        }
    }

    override fun initialize(blockchainInterface: BlockchainInterface<*>) {
        if (blockchainInterface is Migratable) {
            schemaMigrationEngine.executeSchemaUpdate(blockchainInterface.migrations)
        }
    }

    override fun constructAction(blockBundle: BlockBundle): StorageAction {

        val cassandraAction = actionFactories[blockBundle.chain]?.constructCassandraAction(blockBundle)

        if (cassandraAction != null) {
            return SimpleCassandraStorageAction(
                    cassandraStorageAction = cassandraAction,
                    mappingManager = cassandraService.getChainRepository(blockBundle.chain),
                    session = cassandraService.getChainRepositorySession(blockBundle.chain)
            )
        }
        return EmptyStorageAction
    }

    override fun getLastCommittedState(chain: Chain): Long? {
        val applicationId = chainApplicationId(chain)
        return cassandraService.pumpKeyspaceRepository.indexingProgressStore.get(applicationId)?.block_number
    }

    override fun commitState(blockBundle: BlockBundle) {
        log.debug("Commit ${blockBundle.chain} ${blockBundle.number} block indexation")
        val progress = IndexingProgress(
                application_id = chainApplicationId(blockBundle.chain),
                block_number = blockBundle.number, block_hash = blockBundle.hash, index_time = Date()
        )
        cassandraService.pumpKeyspaceRepository.indexingProgressStore.save(progress)
    }
}