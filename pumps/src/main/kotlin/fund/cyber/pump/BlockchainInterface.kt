package fund.cyber.pump

import com.datastax.driver.mapping.MappingManager
import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.node.model.CyberSearchItem
import io.reactivex.Emitter
import io.reactivex.Flowable
import io.reactivex.functions.BiFunction
import java.util.concurrent.Callable

interface Blockchain {
    val chain: Chain
}

interface BlockchainInterface : Blockchain {
    fun blockBundleByNumber(number: Long): BlockBundle
}

interface FlowableBlockchain : Blockchain {
    fun subscribeBlocks(startBlockNumber: Long): Flowable<out BlockBundle>
}

interface BlockBundle {
    val chain: Chain
    val hash: String
    val parentHash: String
    val number: Long
}

class SimpleBlockBundle<T: CyberSearchItem>(
    override val chain: Chain,
    override val hash: String,
    override val parentHash: String,
    override val number: Long,

    val manager: MappingManager
) : BlockBundle {

    var entities: List<out T> = emptyList()
    var actions: List<Pair<()->Unit, ()->Unit>> = emptyList()

    inline fun <reified R:T> push(value: R) {
        val cls = R::class.java
        this.entities += value

        val mapper = manager.mapper(cls)
        add(Pair(
                {mapper.save(value)},
                {mapper.delete(value)}
                )
        )
    }

    fun add(action: Pair<()->Unit, ()->Unit>) {
        actions += action
    }
}

class SerialPulledBlockhain(val blockchain: BlockchainInterface) : FlowableBlockchain, Blockchain{
    override val chain: Chain
        get() = blockchain.chain
    override fun subscribeBlocks(startBlockNumber: Long): Flowable<BlockBundle> {
        return Flowable.generate<BlockBundle, Long>(Callable { startBlockNumber }, downloadNextBlockFunction())

    }

    fun downloadNextBlockFunction() =
            BiFunction { blockNumber: Long, subscriber: Emitter<BlockBundle> ->
                try {
//                log.debug("Pulling block $blockNumber")
                    val block = blockchain.blockBundleByNumber(blockNumber)

                    if (block != null) {
                        subscriber.onNext(block)
                        return@BiFunction blockNumber + 1
                    }
                } catch (e: Exception) {
//                log.error("error during download block $blockNumber", e)
                }
                return@BiFunction blockNumber
            }
}