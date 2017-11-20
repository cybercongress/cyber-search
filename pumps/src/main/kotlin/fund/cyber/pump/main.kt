package fund.cyber.pump

import fund.cyber.dao.system.CassandraSchemaVersionUpdater
import fund.cyber.dao.system.CqlFileBasedMigration
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.runBlocking

import fund.cyber.pump.ethereum_classic.*
import fund.cyber.pump.cassandra.*


val blockchains: Array<BlockchainInterface> = arrayOf(EthereumClassic())

val storages: Array<StorageInterface> = arrayOf(CassandraStorage())

fun main(args: Array<String>) = runBlocking {

    val migrations = listOf(
            CqlFileBasedMigration("pump.bitcoin", 0, "/migrations/bitcoin/0__initial.cql"),
            CqlFileBasedMigration("pump.ethereum", 0, "/migrations/ethereum/0__initial.cql")
    )

    CassandraSchemaVersionUpdater(
            cassandra = AppContext.cassandra, systemDaoService = AppContext.systemDaoService, migrations = migrations
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