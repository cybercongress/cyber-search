package fund.cyber.pump.ethereum.client

import fund.cyber.search.common.hexToLong
import fund.cyber.search.common.sum
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
                transactions = transactions
        )
    }

    private fun parityUnclesToDao(block: EthereumBlock, uncles: List<EthBlock.Block>): List<EthereumUncle> {
        return uncles.mapIndexed { index, uncle ->
            val uncleNumber = uncle.number.toLong()
            EthereumUncle(
                    miner = uncle.miner, hash = uncle.hash, number = uncleNumber, position = index,
                    timestamp = Instant.ofEpochSecond(uncle.timestampRaw.hexToLong()),
                    block_number = block.number, block_time = block.timestamp, block_hash = block.hash,
                    uncle_reward = getUncleReward(chain, uncleNumber, block.number)
            )
        }
    }

    private fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTransaction> {
        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumTransaction(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = BigDecimal(parityTx.value) * weiToEthRate,
                            hash = parityTx.hash, block_hash = parityBlock.hash,
                            block_number = parityBlock.numberRaw.hexToLong(),
                            block_time = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                            creates = parityTx.creates, input = parityTx.input,
                            transaction_index = parityTx.transactionIndexRaw.hexToLong(),
                            gas_limit = parityBlock.gasLimitRaw.hexToLong(),
                            gas_used = parityTx.transactionIndexRaw.hexToLong(),
                            gas_price = BigDecimal(parityTx.gasPrice) * weiToEthRate,
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
                hash = parityBlock.hash, parent_hash = parityBlock.parentHash, number = number,
                miner = parityBlock.miner, difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
                extra_data = parityBlock.extraData, total_difficulty = parityBlock.totalDifficulty,
                gas_limit = parityBlock.gasLimitRaw.hexToLong(), gas_used = parityBlock.gasUsedRaw.hexToLong(),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                logs_bloom = parityBlock.logsBloom, transactions_root = parityBlock.transactionsRoot,
                receipts_root = parityBlock.receiptsRoot, state_root = parityBlock.stateRoot,
                sha3_uncles = parityBlock.sha3Uncles, uncles = parityBlock.uncles,
                tx_number = parityBlock.transactions.size,
                tx_fees = blockTxesFees.sum(), block_reward = blockReward,
                uncles_reward = uncleReward
        )
    }
}


