package fund.cyber.pump.ethereum.client

import fund.cyber.common.DECIMAL_SCALE
import fund.cyber.common.decimal32
import fund.cyber.common.hexToLong
import fund.cyber.common.sum
import fund.cyber.common.toSearchHashFormat
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle
import fund.cyber.search.model.ethereum.getBlockReward
import fund.cyber.search.model.ethereum.getUncleReward
import fund.cyber.search.model.ethereum.weiToEthRate
import org.springframework.stereotype.Component
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.Transaction
import org.web3j.protocol.core.methods.response.TransactionReceipt
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

@Component
class ParityToEthereumBundleConverter(
        private val chain: EthereumFamilyChain
) {

    fun convert(parityBlock: EthBlock.Block, uncles: List<EthBlock.Block>,
                txsReceipts: List<TransactionReceipt>): EthereumBlockBundle {
        val block = parityBlockToDao(parityBlock)
        val blockUncles = parityUnclesToDao(block, uncles)

        val transactions = parityTransactionsToDao(parityBlock, txsReceipts)

        return EthereumBlockBundle(
                hash = parityBlock.hash.toSearchHashFormat(),
                parentHash = parityBlock.parentHash?.toSearchHashFormat() ?: "-1",
                number = parityBlock.number.toLong(), block = block, uncles = blockUncles,
                txes = transactions, blockSize = parityBlock.size.toInt()
        )
    }

    fun parityMempoolTxToDao(parityTx: Transaction): EthereumTx {
        return EthereumTx(
            from = parityTx.from.toSearchHashFormat(), to = parityTx.to.toSearchHashFormat(),
            nonce = parityTx.nonce.toLong(),
            value = BigDecimal(parityTx.value) * weiToEthRate,
            hash = parityTx.hash.toSearchHashFormat(), blockHash = null,
            blockNumber = -1L,
            blockTime = Instant.now(),
            createdSmartContract = parityTx.creates?.toSearchHashFormat(), input = parityTx.input,
            positionInBlock = -1, gasLimit = parityTx.gasRaw.hexToLong(),
            gasUsed = 0,
            gasPrice = BigDecimal(parityTx.gasPrice) * weiToEthRate,
            fee = BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
        )
    }

    private fun parityUnclesToDao(block: EthereumBlock, uncles: List<EthBlock.Block>): List<EthereumUncle> {
        return uncles.mapIndexed { index, uncle ->
            val uncleNumber = uncle.number.toLong()
            EthereumUncle(
                    miner = uncle.miner.toSearchHashFormat(), hash = uncle.hash.toSearchHashFormat(),
                    number = uncleNumber, position = index,
                    timestamp = Instant.ofEpochSecond(uncle.timestampRaw.hexToLong()),
                    blockNumber = block.number, blockTime = block.timestamp,
                    blockHash = block.hash.toSearchHashFormat(),
                    uncleReward = getUncleReward(chain, uncleNumber, block.number)
            )
        }
    }

    private fun parityTransactionsToDao(parityBlock: EthBlock.Block,
                                        txsReceipts: List<TransactionReceipt>): List<EthereumTx> {

        val txReceiptIndex = txsReceipts.associateBy { receipt -> receipt.transactionHash!! }

        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .mapIndexed { index, parityTx ->
                    val gasUsed = txReceiptIndex[parityTx.hash]!!.gasUsedRaw.hexToLong()
                    EthereumTx(
                            from = parityTx.from.toSearchHashFormat(), to = parityTx.to.toSearchHashFormat(),
                            nonce = parityTx.nonce.toLong(), value = BigDecimal(parityTx.value) * weiToEthRate,
                            hash = parityTx.hash.toSearchHashFormat(),
                            blockHash = parityBlock.hash.toSearchHashFormat(),
                            blockNumber = parityBlock.numberRaw.hexToLong(),
                            blockTime = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                            createdSmartContract = parityTx.creates?.toSearchHashFormat(), input = parityTx.input,
                            positionInBlock = index, gasLimit = parityTx.gasRaw.hexToLong(),
                            gasUsed = gasUsed,
                            gasPrice = BigDecimal(parityTx.gasPrice) * weiToEthRate,
                            fee = BigDecimal(parityTx.gasPrice * gasUsed.toBigInteger()) * weiToEthRate
                    )
                }
    }


    private fun parityBlockToDao(parityBlock: EthBlock.Block): EthereumBlock {
        val blockTxesFees = parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
                }

        val number = parityBlock.numberRaw.hexToLong()
        val blockReward = getBlockReward(chain, number)
        val uncleReward = (blockReward * parityBlock.uncles.size.toBigDecimal())
                .divide(decimal32, DECIMAL_SCALE, RoundingMode.FLOOR).stripTrailingZeros()

        return EthereumBlock(
                hash = parityBlock.hash.toSearchHashFormat(), parentHash = parityBlock.parentHash.toSearchHashFormat(),
                number = number, minerContractHash = parityBlock.miner.toSearchHashFormat(),
                difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
                extraData = parityBlock.extraData, totalDifficulty = parityBlock.totalDifficulty,
                gasLimit = parityBlock.gasLimitRaw.hexToLong(), gasUsed = parityBlock.gasUsedRaw.hexToLong(),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                logsBloom = parityBlock.logsBloom, transactionsRoot = parityBlock.transactionsRoot,
                receiptsRoot = parityBlock.receiptsRoot, stateRoot = parityBlock.stateRoot,
                sha3Uncles = parityBlock.sha3Uncles.toSearchHashFormat(), uncles = parityBlock.uncles,
                txNumber = parityBlock.transactions.size, nonce = parityBlock.nonce.toLong(),
                txFees = blockTxesFees.sum(), blockReward = blockReward, unclesReward = uncleReward
        )
    }
}


