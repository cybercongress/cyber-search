package fund.cyber.pump.ethereum

import fund.cyber.node.common.Chain
import fund.cyber.node.common.hexToLong
import fund.cyber.node.common.sumByBigDecimal
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.getBlockReward
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigDecimal
import java.time.Instant

class ParityToEthereumBundleConverter(private val chain: Chain) {

    private val weiToEthRate = BigDecimal("1E-18")

    fun convertToBundle(parityBlock: EthBlock.Block): EthereumBlockBundle {

        val transactions = parityTransactionsToDao(parityBlock)
        val block = parityBlockToDao(parityBlock)

        return EthereumBlockBundle(
                hash = parityBlock.hash, parentHash = parityBlock.parentHash ?: "-1",
                number = parityBlock.number.toLong(), block = block, transactions = transactions,
                chain = chain
        )
    }

    private fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTransaction> {

        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumTransaction(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = (BigDecimal(parityTx.value) * weiToEthRate).toString(),
                            timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                            hash = parityTx.hash, block_hash = parityBlock.hash,
                            block_number = parityBlock.numberRaw.hexToLong(),
                            creates = parityTx.creates, input = parityTx.input,
                            transaction_index = parityTx.transactionIndexRaw.hexToLong(),
                            gas_limit = parityBlock.gasLimitRaw.hexToLong(),
                            gas_used = parityTx.transactionIndexRaw.hexToLong(),
                            gas_price = BigDecimal(parityTx.gasPrice) * weiToEthRate,
                            fee = (BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate).toString()
                    )
                }
    }

    private fun parityBlockToDao(parityBlock: EthBlock.Block): EthereumBlock {

        val blockTxes = parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
                }

        val blockTxFees = blockTxes.sumByBigDecimal { it }.toString()
        val number = parityBlock.numberRaw.hexToLong()

        return EthereumBlock(
                hash = parityBlock.hash, parent_hash = parityBlock.parentHash, number = number,
                miner = parityBlock.miner, difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
                extra_data = parityBlock.extraData, total_difficulty = parityBlock.totalDifficulty,
                gas_limit = parityBlock.gasLimitRaw.hexToLong(), gas_used = parityBlock.gasUsedRaw.hexToLong(),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
                logs_bloom = parityBlock.logsBloom, transactions_root = parityBlock.transactionsRoot,
                receipts_root = parityBlock.receiptsRoot, state_root = parityBlock.stateRoot,
                sha3_uncles = parityBlock.sha3Uncles, uncles = parityBlock.uncles,
                tx_number = blockTxes.size, tx_fees = blockTxFees, block_reward = getBlockReward(number).toString()
        )
    }
}

