package fund.cyber.pump.common.node

import fund.cyber.common.StackCache
import fund.cyber.search.model.events.PumpEvent
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

interface BlockBundleEventGenerator<T : BlockBundle> {
    fun generate(blockBundle: T, history: StackCache<T>): List<Pair<PumpEvent, T>>
}

private val log = LoggerFactory.getLogger(ChainReorganizationBlockBundleEventGenerator::class.java)!!

@Component
class ChainReorganizationBlockBundleEventGenerator<T : BlockBundle>(
        private val blockchainInterface: FlowableBlockchainInterface<T>,
        monitoring: MeterRegistry
) : BlockBundleEventGenerator<T> {

    private val chainReorganizationMonitor = monitoring.counter("pump_chain_reorganization_counter")

    override fun generate(blockBundle: T, history: StackCache<T>): List<Pair<PumpEvent, T>> {
        val exHash = history.peek()?.hash ?: ""
        if (exHash.isNotEmpty() && blockBundle.parentHash != exHash) {
            log.info("Chain reorganization occurred. Processing involved bundles")
            chainReorganizationMonitor.increment()
            return getReorganizationBundles(blockBundle, history)
        }
        history.push(blockBundle)
        return listOf(PumpEvent.NEW_BLOCK to blockBundle)
    }

    private fun getReorganizationBundles(blockBundle: T, history: StackCache<T>): List<Pair<PumpEvent, T>> {
        var tempBlockBundle = blockBundle
        var prevBlockBundle: T? = null

        var newBlocks = listOf(PumpEvent.NEW_BLOCK to tempBlockBundle)
        var revertBlocks = listOf<Pair<PumpEvent, T>>()

        log.info("Starting reorganization...")
        log.info("New block: {number: ${tempBlockBundle.number}, hash: ${tempBlockBundle.hash}," +
                " parentHash: ${tempBlockBundle.parentHash}}")
        do {
            if (prevBlockBundle != null) {
                revertBlocks += PumpEvent.DROPPED_BLOCK to prevBlockBundle
                tempBlockBundle = blockchainInterface.blockBundleByNumber(tempBlockBundle.number - 1L)
                newBlocks += PumpEvent.NEW_BLOCK to tempBlockBundle

                log.info("Block to revert: {number: ${prevBlockBundle.number}, hash: ${prevBlockBundle.hash}," +
                        " parentHash: ${prevBlockBundle.parentHash}}")
                log.info("New block: {number: ${tempBlockBundle.number}, hash: ${tempBlockBundle.hash}," +
                        " parentHash: ${tempBlockBundle.parentHash}}")
            }

            prevBlockBundle = history.pop()
            if (prevBlockBundle == null) {
                throw HistoryStackIsEmptyException("History stack is empty while chain reorganization is not" +
                        " finished yet! Please adjust stack capacity and try again.")
            }
        } while (prevBlockBundle?.hash != tempBlockBundle.parentHash)

        newBlocks = newBlocks.reversed()
        newBlocks.forEach { history.push(it.second) }

        log.info("Finishing reorganization... Total blocks to revert: ${revertBlocks.size};" +
                " Total new blocks: ${newBlocks.size}")

        return (revertBlocks + newBlocks)
    }
}

class HistoryStackIsEmptyException(message: String) : Exception(message)
