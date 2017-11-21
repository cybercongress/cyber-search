package fund.cyber.pump.ethereum_classic

import fund.cyber.pump.Block
import fund.cyber.pump.BlockchainInterface
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService
import rx.Observable
import java.math.BigInteger
import java.util.concurrent.Executors

val PARITY_URL = "http://127.0.0.1:8545"

const val BATCH_SIZE_DEFAULT: Long = 32

class EthereumClassic: BlockchainInterface {

    private var batchSize: BigInteger = BigInteger.valueOf(BATCH_SIZE_DEFAULT)
    private lateinit var lastNetworkBlock: BigInteger
    private lateinit var lastParsedBlockNumber: BigInteger

    override val blocks: Observable<Block>
        get() {
            val executorService = Executors.newScheduledThreadPool(batchSize.toInt())

            val parityClient = Web3j.build(HttpService(PARITY_URL), 15 * 1000, executorService)

            lastParsedBlockNumber = this.lastParsedBlockNumber()
            lastNetworkBlock = parityClient.ethBlockNumber().send().blockNumber
            println(lastNetworkBlock)
            return parityClient.catchUpToLatestAndSubscribeToNewBlocksObservable({"0"}, true).map { block ->  EthereumClassicBlock(block) }

        }

    private fun lastParsedBlockNumber(): BigInteger {
//        val blockNumber = context.offsetStorageReader()?.offset(sourcePartition)?.get("blockNumber") ?: return BigInteger.ZERO
        return BigInteger.ZERO
    }
}