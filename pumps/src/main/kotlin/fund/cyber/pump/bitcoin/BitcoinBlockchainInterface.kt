package fund.cyber.pump.bitcoin

import fund.cyber.node.common.Chain.BITCOIN
import fund.cyber.node.model.JsonRpcBitcoinBlock
import fund.cyber.pump.BlockchainInterface
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

private val log = LoggerFactory.getLogger(BitcoinBlockchainInterface::class.java)!!


class BitcoinBlockchainInterface : BlockchainInterface<JsonRpcBitcoinBlock> {

    override val chain = BITCOIN

    override fun subscribeBlocks(startBlockNumber: Long) =
            Flowable.generate<JsonRpcBitcoinBlock, Long>(Callable { 0L }, downloadNextBlockFunction())!!
}


fun downloadNextBlockFunction(btcdClient: BitcoinJsonRpcClient = BitcoinPumpContext.bitcoinJsonRpcClient) =
        BiFunction { blockNumber: Long, subscriber: Emitter<JsonRpcBitcoinBlock> ->
            try {
                log.debug("Pulling block $blockNumber")
                val block = btcdClient.getBlockByNumber(blockNumber)
                if (block != null) {
                    subscriber.onNext(block)
                    return@BiFunction blockNumber + 1
                }
            } catch (e: Exception) {
                log.error("error during download block $blockNumber", e)
            }
            return@BiFunction blockNumber
        }