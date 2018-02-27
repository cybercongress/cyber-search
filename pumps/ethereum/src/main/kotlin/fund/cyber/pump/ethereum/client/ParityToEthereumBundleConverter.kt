package fund.cyber.pump.ethereum.client

import fund.cyber.common.hexToLong
import fund.cyber.common.sum
import fund.cyber.search.model.chains.EthereumFamilyChain
import fund.cyber.search.model.ethereum.*
import org.springframework.stereotype.Component
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

@Component
class ParityToEthereumBundleConverter(
        private val chain: EthereumFamilyChain
) {

    fun convert(parityBlock: EthBlock.Block, uncles: List<EthBlock.Block>): EthereumBlockBundle {
        val block = parityBlockToDao(parityBlock)
        val blockUncles = parityUnclesToDao(block, uncles)

        val transactions = parityTransactionsToDao(parityBlock)

        return EthereumBlockBundle(
                hash = parityBlock.hash, parentHash = parityBlock.parentHash ?: "-1",
                number = parityBlock.number.toLong(), block = block, uncles = blockUncles,
                txes = transactions, blockSize = parityBlock.size.toInt()
        )
    }

    private fun parityUnclesToDao(block: EthereumBlock, uncles: List<EthBlock.Block>): List<EthereumUncle> {
        return uncles.mapIndexed { index, uncle ->
            val uncleNumber = uncle.number.toLong()
            EthereumUncle(
                    miner = uncle.miner, hash = uncle.hash, number = uncleNumber, position = index,
                    timestamp = Instant.ofEpochSecond(uncle.timestampRaw.hexToLong()),
                    blockNumber = block.number, blockTime = block.timestamp, blockHash = block.hash,
                    uncleReward = getUncleReward(chain, uncleNumber, block.number)
            )
        }
    }

    private fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTx> {
        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .mapIndexed { index, parityTx ->
                    EthereumTx(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = BigDecimal(parityTx.value) * weiToEthRate,
                            hash = parityTx.hash, blockHash = parityBlock.hash,
                            blockNumber = parityBlock.numberRaw.hexToLong(),
                            blockTime = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                            createdContract = parityTx.creates, input = parityTx.input,
                            positionInBlock = index, gasLimit = parityBlock.gasLimitRaw.hexToLong(),
                            gasUsed = parityTx.transactionIndexRaw.hexToLong(),
                            gasPrice = BigDecimal(parityTx.gasPrice) * weiToEthRate,
                            fee = BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
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
                .divide(32.toBigDecimal(), 18, RoundingMode.FLOOR).stripTrailingZeros()

        return EthereumBlock(
                hash = parityBlock.hash, parentHash = parityBlock.parentHash, number = number,
                miner = parityBlock.miner, difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
                extraData = parityBlock.extraData, totalDifficulty = parityBlock.totalDifficulty,
                gasLimit = parityBlock.gasLimitRaw.hexToLong(), gasUsed = parityBlock.gasUsedRaw.hexToLong(),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                logsBloom = parityBlock.logsBloom, transactionsRoot = parityBlock.transactionsRoot,
                receiptsRoot = parityBlock.receiptsRoot, stateRoot = parityBlock.stateRoot,
                sha3Uncles = parityBlock.sha3Uncles, uncles = parityBlock.uncles,
                txNumber = parityBlock.transactions.size, nonce = parityBlock.nonce.toLong(),
                txFees = blockTxesFees.sum(), blockReward = blockReward, unclesReward = uncleReward
        )
    }
}


