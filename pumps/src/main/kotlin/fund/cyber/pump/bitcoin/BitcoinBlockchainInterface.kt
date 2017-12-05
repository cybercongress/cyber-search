package fund.cyber.pump.bitcoin

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.model.BitcoinBlock
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.pump.BlockBundle
import fund.cyber.pump.BlockchainInterface
import fund.cyber.pump.FlowableBlockchain
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

private val log = LoggerFactory.getLogger(BitcoinBlockchainInterface::class.java)!!


class BitcoinBlockBundle(
        override val hash: String,
        override val parentHash: String,
        override val number: Long,
        override val chain: Chain,
        val block: BitcoinBlock,
        val transactions: List<BitcoinTransaction>
) : BlockBundle

class BitcoinBlockchainInterface : BlockchainInterface, FlowableBlockchain {
    override val lastNetowrkBlock: Long
        get() = BitcoinPumpContext.bitcoinJsonRpcClient.getLastBlockNumber()
    private val downloadNextBlockFunction = DownloadNextBlockFunction(BitcoinPumpContext.bitcoinJsonRpcClient)
    override val chain = BITCOIN

    override fun subscribeBlocks(startBlockNumber: Long) =
            Flowable.generate<List<JsonRpcBitcoinBlock>, Long>(Callable { startBlockNumber }, downloadNextBlockFunction)
                    .flatMapIterable { items -> items }
                    .map(BitcoinPumpContext.jsonRpcToDaoBitcoinEntitiesConverter::convertToBundle)!!

    override fun blockBundleByNumber(number: Long): BlockBundle {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}


class DownloadNextBlockFunction(
        private val client: BitcoinJsonRpcClient
) : BiFunction<Long, Emitter<List<JsonRpcBitcoinBlock>>, Long> {

    private var lastNetworkBlock: AtomicLong = AtomicLong(client.getLastBlockNumber())


    override fun apply(blockNumber: Long, subscriber: Emitter<List<JsonRpcBitcoinBlock>>): Long {
        try {

            val isBatchFetch = lastNetworkBlock.get() - blockNumber > 5
            val downloadCount = if (isBatchFetch) 4 else 1

            if (!isBatchFetch) {
                val lastBlockNumber = client.getLastBlockNumber()
                lastNetworkBlock.set(lastBlockNumber)
                if (blockNumber == lastBlockNumber) {
                    log.debug("Up-to-date block $blockNumber")
                    return blockNumber
                }
            }

            log.debug("Looking for $blockNumber-${blockNumber + downloadCount - 1} blocks")

            val blockNumbers = (blockNumber until blockNumber + downloadCount).toList()
            subscriber.onNext(client.getBlocksByNumber(blockNumbers))
            return blockNumber + downloadCount

        } catch (e: Exception) {
            log.error("error during download block $blockNumber", e)
        }
        return blockNumber
    }
}