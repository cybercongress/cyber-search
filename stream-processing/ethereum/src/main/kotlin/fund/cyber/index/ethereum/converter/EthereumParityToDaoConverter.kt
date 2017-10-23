package fund.cyber.index.ethereum.converter

import fund.cyber.node.common.hexToLong
import fund.cyber.node.common.sumByBigDecimal
import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumBlockTransaction
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.getBlockReward
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigDecimal
import java.time.Instant

class EthereumParityToDaoConverter {

    private val weiToEthRate = BigDecimal("1E-18")

    fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTransaction> {

        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumTransaction(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = (BigDecimal(parityTx.value) * weiToEthRate).toString(),
                            timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()).toString(),
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


    fun parityBlockToDao(parityBlock: EthBlock.Block): EthereumBlock {

        val blockTxes = parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->

                    val to = if (parityTx.creates != null) parityTx.creates else parityTx.to

                    EthereumBlockTransaction(
                            from = parityTx.from, to = to, hash = parityTx.hash,
                            amount = BigDecimal(parityTx.value) * weiToEthRate,
                            fee = BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate,
                            creates_contract = parityTx.creates != null
                    )

                }

        val blockTxFees = blockTxes.sumByBigDecimal { tx -> tx.fee }.toString()
        val number = parityBlock.numberRaw.hexToLong()

        return EthereumBlock(
                hash = parityBlock.hash, parent_hash = parityBlock.parentHash, number = number,
                miner = parityBlock.miner, difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.hexToLong(),
                extra_data = parityBlock.extraData, total_difficulty = parityBlock.totalDifficulty,
                gas_limit = parityBlock.gasLimitRaw.hexToLong(), gas_used = parityBlock.gasUsedRaw.hexToLong(),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()).toString(),
                logs_bloom = parityBlock.logsBloom, transactions_root = parityBlock.transactionsRoot,
                receipts_root = parityBlock.receiptsRoot, state_root = parityBlock.stateRoot,
                sha3_uncles = parityBlock.sha3Uncles, uncles = parityBlock.uncles, transactions = blockTxes,
                tx_number = blockTxes.size, tx_fees = blockTxFees, block_reward = getBlockReward(number).toString()
        )
    }
}
