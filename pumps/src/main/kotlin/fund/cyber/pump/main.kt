package fund.cyber.pump

import fund.cyber.dao.migration.CqlFileBasedMigration
import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.dao.migration.ElasticHttpMigration
import fund.cyber.pump.cassandra.CassandraStorage
import fund.cyber.pump.ethereum_classic.EthereumClassic
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking


val blockchains: Array<BlockchainInterface> = arrayOf(EthereumClassic())

val storages: Array<StorageInterface> = arrayOf(CassandraStorage())

fun main(args: Array<String>) = runBlocking {


    val bitcoinMigrations = listOf(
            CqlFileBasedMigration(0, "pump.bitcoin", "/migrations/bitcoin/0_initial.cql"),
            ElasticHttpMigration(1, "pump.bitcoin", "/migrations/bitcoin/1_create-tx-index.json"),
            ElasticHttpMigration(2, "pump.bitcoin", "/migrations/bitcoin/2_create-tx-type.json")
    )

    ElassandraSchemaMigrationEngine(
            cassandra = AppContext.cassandra, httpClient = AppContext.httpClient,
            elasticHost = AppContext.pumpsConfiguration.cassandraServers.first(),
            elasticPort = AppContext.pumpsConfiguration.elasticHttpPort,
            systemDaoService = AppContext.systemDaoService, migrations = bitcoinMigrations
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