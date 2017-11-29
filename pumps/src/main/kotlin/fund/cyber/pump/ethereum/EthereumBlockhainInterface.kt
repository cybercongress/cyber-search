package fund.cyber.pump.ethereum

import fund.cyber.node.common.Chain
import fund.cyber.node.model.*
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import org.web3j.protocol.Web3j
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.http.HttpService
import java.math.BigInteger
import java.util.concurrent.Callable
import java.util.concurrent.Executors

class EthereumBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: EthereumBlock,
        val transactions: List<EthereumTransaction>
) : BlockBundle

const val BATCH_SIZE_DEFAULT: Long = 32

class EthereumBlockhainInterface(url: String, override val chain: Chain) : BlockchainInterface<EthereumBlockBundle> {
//    override val chain: Chain = Chain.ETHEREUM
    private var batchSize: BigInteger = BigInteger.valueOf(BATCH_SIZE_DEFAULT)
    private val executorService = Executors.newScheduledThreadPool(batchSize.toInt())

    private val parityToDaoConverter = EthereumParityToDaoConverter()

    val parityClient = Web3j.build(HttpService(url), 15 * 1000, executorService)

    override fun subscribeBlocks(startBlockNumber: Long): Flowable<EthereumBlockBundle> {
        return Flowable.generate<EthBlock, Long>(Callable { startBlockNumber }, downloadNextBlockFunction())
                .map { _block ->
                    val block = parityToDaoConverter.parityBlockToDao(_block.block)
                    EthereumBlockBundle(
                            hash = block.hash,
                            parentHash = block.parent_hash,
                            number = block.number,
                            chain = this.chain,
                            block = block,
                            transactions = parityToDaoConverter.parityTransactionsToDao(_block.block)
                    )
                }
    }

    fun downloadNextBlockFunction() =
            BiFunction { blockNumber: Long, subscriber: Emitter<EthBlock> ->
                try {
//                log.debug("Pulling block $blockNumber")
                    val block = parityClient.ethGetBlockByNumber(this.blockParameter(java.math.BigInteger(blockNumber.toString())), true).send()

                    if (block != null) {
                        subscriber.onNext(block)
                        return@BiFunction blockNumber + 1
                    }
                } catch (e: Exception) {
//                log.error("error during download block $blockNumber", e)
                }
                return@BiFunction blockNumber
            }

    private fun blockParameter(blockNumber: BigInteger) = DefaultBlockParameter.valueOf(blockNumber)!!
}

