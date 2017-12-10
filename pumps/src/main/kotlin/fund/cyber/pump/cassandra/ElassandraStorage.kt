package fund.cyber.pump.cassandra

import fund.cyber.cassandra.CassandraService
import fund.cyber.cassandra.migration.ElassandraSchemaMigrationEngine
import fund.cyber.cassandra.migration.Migratory
import fund.cyber.node.common.Chain
import fund.cyber.node.model.IndexingProgress
import fund.cyber.pump.*
import org.apache.http.client.HttpClient
import java.util.*


class ElassandraStorage(
        private val cassandraService: CassandraService,
        httpClient: HttpClient, elasticHost: String, elasticHttpPort: Int
) : StorageInterface, StateStorage {


    private val schemaMigrationEngine = ElassandraSchemaMigrationEngine(
            cassandraService = cassandraService, httpClient = httpClient,
            elasticHost = elasticHost, elasticPort = elasticHttpPort,
            defaultMigrations = PumpsMigrations.migrations
    )

    private val actionFactories = mutableMapOf<Chain, CassandraStorageActionSourceFactory<BlockBundle>>()

    @Suppress("UNCHECKED_CAST")
    override fun registerStorageActionSourceFactory(chain: Chain, actionSourceFactory: StorageActionSourceFactory) {
        if (actionSourceFactory is CassandraStorageActionSourceFactory<*>) {
            actionFactories.put(chain, actionSourceFactory as CassandraStorageActionSourceFactory<BlockBundle>)
        }
    }

    override fun initialize(blockchainInterface: BlockchainInterface<*>) {
        if (blockchainInterface is Migratory) {
            schemaMigrationEngine.executeSchemaUpdate(blockchainInterface.migrations)
        }
    }

    override fun constructAction(blockBundle: BlockBundle): StorageAction {

        val cassandraAction = actionFactories[blockBundle.chain]?.constructCassandraAction(blockBundle)

        if (cassandraAction != null) {
            return CassandraStorageAction(
                    storageActionSource = cassandraAction,
                    keyspaceRepository = cassandraService.getChainRepository(blockBundle.chain)
            )
        }
        return EmptyStorageAction
    }

    override fun getLastCommittedState(chain: Chain): Long? {
        val applicationId = chain.chainApplicationId
        return cassandraService.pumpKeyspaceRepository.indexingProgressStore.get(applicationId)?.block_number
    }

    override fun commitState(blockBundle: BlockBundle) {
        val progress = IndexingProgress(
                application_id = blockBundle.chain.chainApplicationId,
                block_number = blockBundle.number, block_hash = blockBundle.hash, index_time = Date()
        )
        cassandraService.pumpKeyspaceRepository.indexingProgressStore.save(progress)
    }
}