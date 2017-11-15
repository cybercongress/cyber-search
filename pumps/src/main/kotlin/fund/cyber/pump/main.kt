package fund.cyber.pump

import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.runBlocking

import fund.cyber.pump.ethereum_classic.*
import fund.cyber.pump.cassandra.*


val blockchains: Array<BlockchainInterface> = arrayOf(EthereumClassic())

val storages: Array<StorageInterface> = arrayOf(CassandraStorage())

fun main(args: Array<String>) = runBlocking<Unit> {

    val loop = launch {
        storages.forEach { storage ->
            blockchains.forEach { blockchain ->
                storage.initFor(blockchain)
            }
        }

        blockchains.forEach { blockchain ->
            blockchain.blocks.subscribe { block ->

                storages.forEach { storage ->
                    storage.store(block)
                }

            }
        }

    }
    loop.join()
}
