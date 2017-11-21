package fund.cyber.pump

import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.pump.cassandra.CassandraStorage
import fund.cyber.pump.ethereum_classic.EthereumClassic
import fund.cyber.pump.ethereum_classic.EthereumClassicMigrations
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking


val blockchains: Array<BlockchainInterface> = arrayOf(EthereumClassic())

val storages: Array<StorageInterface> = arrayOf(CassandraStorage())

fun main(args: Array<String>) = runBlocking {

    ElassandraSchemaMigrationEngine(
            cassandra = AppContext.cassandra, httpClient = AppContext.httpClient,
            elasticHost = AppContext.pumpsConfiguration.cassandraServers.first(),
            elasticPort = AppContext.pumpsConfiguration.elasticHttpPort,
            systemDaoService = AppContext.systemDaoService, migrations = EthereumClassicMigrations.migrations
    ).executeSchemaUpdate()

    val loop = launch {
        storages.forEach { storage ->
            blockchains.forEach { blockchain ->
                storage.initFor(blockchain)
            }
        }

        blockchains.forEach { blockchain ->
            blockchain.blocks.subscribe { block ->

                storages.forEach { storage ->
                    val action = storage.store(block)
                }

            }
        }

    }
    loop.join()
}