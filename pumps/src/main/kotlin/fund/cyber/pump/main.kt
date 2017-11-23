package fund.cyber.pump

import fund.cyber.dao.migration.ElassandraSchemaMigrationEngine
import fund.cyber.pump.cassandra.CassandraStorage
import fund.cyber.pump.ethereum.EthereumMigrations
import fund.cyber.pump.ethereum_classic.EthereumClassic
import kotlinx.coroutines.experimental.runBlocking


val blockchains: Array<BlockchainInterface> = arrayOf(EthereumClassic())

val storages: Array<StorageInterface> = arrayOf(CassandraStorage())

fun main(args: Array<String>) = runBlocking {

//    val loop = launch {
    storages.forEach { storage ->
        blockchains.forEach { blockchain ->
            storage.initFor(blockchain)
        }
    }

    blockchains.forEach { blockchain ->
        blockchain.blocks.subscribe { block ->

            storages.forEach { storage ->
                val action = storage.actionFor(block)

                action.store()
            }

        }
    }

//    }
//    loop.join()
}