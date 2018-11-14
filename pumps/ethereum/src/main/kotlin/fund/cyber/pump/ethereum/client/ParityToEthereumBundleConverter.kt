package fund.cyber.pump.ethereum.client

import fund.cyber.common.decimal32
import fund.cyber.common.hexToLong
import fund.cyber.common.sum
import fund.cyber.common.toSearchHashFormat
import fund.cyber.search.model.ethereum.ErroredOperationResult
import fund.cyber.search.model.chains.ChainInfo
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.ethereum.TxTrace
import fund.cyber.search.model.ethereum.weiToEthRate
import org.springframework.stereotype.Component
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.Transaction
import org.web3j.protocol.core.methods.response.TransactionReceipt
import org.web3j.protocol.parity.methods.response.Trace
import java.math.BigDecimal
import java.time.Instant

@Component
class ParityToEthereumBundleConverter(
    private val chainInfo: ChainInfo
) {


    fun convert(rawData: BundleRawData): EthereumBlockBundle {

        val transactions = parityTransactionsToDao(rawData)
        val block = parityBlockToDao(rawData.block, transactions, rawData.calls)
        val blockUncles = parityUnclesToDao(block, rawData.uncles, rawData.calls)

        //todo parent hash test, reorganisation
        return EthereumBlockBundle(
            hash = block.hash.toSearchHashFormat(), parentHash = block.parentHash.toSearchHashFormat(),
            number = block.number, block = block, uncles = blockUncles,
            txes = transactions, blockSize = block.size.toInt()
        )
    }

    fun parityMempoolTxToDao(parityTx: Transaction): EthereumTx {
        return EthereumTx(
            from = parityTx.from.toSearchHashFormat(), to = parityTx.to?.toSearchHashFormat(),
            nonce = parityTx.nonce.toLong(), error = null,
            value = BigDecimal(parityTx.value) * weiToEthRate,
            hash = parityTx.hash.toSearchHashFormat(), blockHash = null,
            blockNumber = -1L, firstSeenTime = Instant.now(), blockTime = null,
            createdSmartContract = parityTx.creates?.toSearchHashFormat(), input = parityTx.input,
            positionInBlock = -1, gasLimit = parityTx.gasRaw.hexToLong(),
            gasUsed = 0, trace = null,
            gasPrice = BigDecimal(parityTx.gasPrice) * weiToEthRate,
            fee = BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
        )
    }

    private fun parityUnclesToDao(block: EthereumBlock, uncles: List<EthBlock.Block>, traces: List<Trace>)
        : List<EthereumUncle>
    {
        return uncles.mapIndexed { index, uncle ->
            val uncleNumber = uncle.number.toLong()
            EthereumUncle(
                miner = uncle.miner.toSearchHashFormat(), hash = uncle.hash.toSearchHashFormat(),
                number = uncleNumber, position = index,
                timestamp = Instant.ofEpochSecond(uncle.timestampRaw.hexToLong()),
                blockNumber = block.number, blockTime = block.timestamp,
                blockHash = block.hash.toSearchHashFormat(),
                uncleReward = getUncleReward(traces, uncle.miner.toSearchHashFormat(), index)
            )
        }
    }

    private fun parityTransactionsToDao(rawData: BundleRawData): List<EthereumTx> {

        val parityBlock = rawData.block
        val txReceiptIndex = rawData.txsReceipts.associateBy { receipt -> receipt.transactionHash!! }
        val tracesIndex = toTxesTraces(rawData.calls)

        return parityBlock.transactions
            .filterIsInstance<EthBlock.TransactionObject>()
            .mapIndexed { index, parityTx ->
                val gasUsed = txReceiptIndex[parityTx.hash]!!.gasUsedRaw.hexToLong()
                EthereumTx(
                    from = parityTx.from.toSearchHashFormat(), to = parityTx.to?.toSearchHashFormat(),
                    nonce = parityTx.nonce.toLong(), value = BigDecimal(parityTx.value) * weiToEthRate,
                    hash = parityTx.hash.toSearchHashFormat(),
                    error = txError(txReceiptIndex[parityTx.hash]!!, tracesIndex[parityTx.hash]!!),
                    blockHash = parityBlock.hash.toSearchHashFormat(),
                    blockNumber = parityBlock.numberRaw.hexToLong(),
                    firstSeenTime = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                    blockTime = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                    createdSmartContract = parityTx.creates?.toSearchHashFormat(), input = parityTx.input,
                    positionInBlock = index, gasLimit = parityTx.gasRaw.hexToLong(),
                    gasUsed = gasUsed, trace = tracesIndex[parityTx.hash],
                    gasPrice = BigDecimal(parityTx.gasPrice) * weiToEthRate,
                    fee = BigDecimal(parityTx.gasPrice * gasUsed.toBigInteger()) * weiToEthRate
                )
            }
    }

    /**
     * Returns error for failed txes, or null if tx succeeded
     */
    private fun txError(receipt: TransactionReceipt, txTrace: TxTrace): String? {

        if (receipt.isStatusOK) return null
        return (txTrace.rootOperationTrace.result as ErroredOperationResult).error
    }


    private fun parityBlockToDao(parityBlock: EthBlock.Block, transactions: List<EthereumTx>, traces: List<Trace>)
        : EthereumBlock
    {
        val blockTxesFees = transactions.map { tx -> tx.fee }

        val number = parityBlock.numberRaw.hexToLong()
        val blockReward = getBlockReward(traces)
        val uncleInclusionReward = blockReward.multiply(parityBlock.uncles.size.toBigDecimal())
            .divide(decimal32.plus(parityBlock.uncles.size.toBigDecimal())).stripTrailingZeros()
        val staticBlockReward = blockReward.minus(uncleInclusionReward).stripTrailingZeros()

        return EthereumBlock(
            hash = parityBlock.hash.toSearchHashFormat(), parentHash = parityBlock.parentHash.toSearchHashFormat(),
            number = number, minerContractHash = parityBlock.miner.toSearchHashFormat(),
            difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
            extraData = parityBlock.extraData.toSearchHashFormat(), totalDifficulty = parityBlock.totalDifficulty,
            gasLimit = parityBlock.gasLimitRaw.hexToLong(), gasUsed = parityBlock.gasUsedRaw.hexToLong(),
            timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
            logsBloom = parityBlock.logsBloom.toSearchHashFormat(),
            transactionsRoot = parityBlock.transactionsRoot.toSearchHashFormat(),
            receiptsRoot = parityBlock.receiptsRoot.toSearchHashFormat(),
            stateRoot = parityBlock.stateRoot.toSearchHashFormat(),
            sha3Uncles = parityBlock.sha3Uncles.toSearchHashFormat(), uncles = parityBlock.uncles,
            txNumber = parityBlock.transactions.size, nonce = parityBlock.nonce.toLong(),
            txFees = blockTxesFees.sum(), blockReward = staticBlockReward,
            unclesReward = uncleInclusionReward
        )
    }

    fun getRewardTraces(traces: List<Trace>): List<Trace.RewardAction> {
        return traces
            .map { trace -> trace.action }
            .filterIsInstance<Trace.RewardAction>()
    }

    fun getBlockReward(traces: List<Trace>): BigDecimal {
        val blockReward = getRewardTraces(traces)
            .filter { rewardAction -> rewardAction.rewardType == "block" }
        return if (blockReward.isNotEmpty()) blockReward[0].value.toBigDecimal() * weiToEthRate else BigDecimal.ZERO
    }

    fun getUncleReward(traces: List<Trace>, miner: String, unclePosition: Int): BigDecimal {
        val uncleRewardTracesByMiner = getRewardTraces(traces)
            .filter { rewardAction -> 
                rewardAction.rewardType == "uncle" && rewardAction.author.toSearchHashFormat() == miner}
        if (uncleRewardTracesByMiner.size == 1)
            return uncleRewardTracesByMiner[0].value.toBigDecimal() * weiToEthRate
        return if (uncleRewardTracesByMiner.isNotEmpty())
            uncleRewardTracesByMiner[unclePosition].value.toBigDecimal() * weiToEthRate else BigDecimal.ZERO
    }
}
