package fund.cyber.index.ethereum.converter

import fund.cyber.node.model.EthereumBlock
import fund.cyber.node.model.EthereumBlockTransaction
import fund.cyber.node.model.EthereumTransaction
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigDecimal
import java.time.Instant

class EthereumParityToDaoConverter() {

    private val weiToEthRate = BigDecimal("1E-18")

    fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTransaction> {

        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumTransaction(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = (BigDecimal(parityTx.value) * weiToEthRate).toString(),
                            timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.toLong(16)).toString(),
                            hash = parityTx.hash, block_hash = parityBlock.hash,
                            block_number = parityBlock.numberRaw.toLong(16),
                            creates = parityTx.creates, input = parityTx.input,
                            transaction_index = parityTx.transactionIndexRaw.toLong(16),
                            gas_limit = parityBlock.gasLimitRaw.toLong(16),
                            gas_used = parityTx.transactionIndexRaw.toLong(16),
                            gas_price =BigDecimal(parityTx.gasPrice) * weiToEthRate,
                            fee = (BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate).toString()
                    )
                }
    }

    fun parityBlockToDao(parityBlock: EthBlock.Block): EthereumBlock {

        val blockTxes = parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumBlockTransaction(
                            from = parityTx.from, to = parityTx.to, hash = parityTx.hash,
                            amount = BigDecimal(parityTx.value) * weiToEthRate,
                            fee = BigDecimal(parityTx.gasPrice * parityTx.gas) * weiToEthRate
                    )

                }

        return EthereumBlock(
                hash = parityBlock.hash, parent_hash = parityBlock.parentHash, number = parityBlock.numberRaw.toLong(16),
                miner = parityBlock.miner, difficulty = parityBlock.difficulty, size = parityBlock.sizeRaw.toLong(16),
                extra_data = parityBlock.extraData, total_difficulty = parityBlock.totalDifficulty,
                gas_limit = parityBlock.gasLimitRaw.toLong(16), gas_used = parityBlock.gasUsedRaw.toLong(16),
                timestamp = Instant.ofEpochSecond(parityBlock.timestampRaw.toLong(16)).toString(),
                logs_bloom = parityBlock.logsBloom, transactions_root = parityBlock.transactionsRoot,
                receipts_root = parityBlock.receiptsRoot, state_root = parityBlock.stateRoot,
                sha3_uncles = parityBlock.sha3Uncles, uncles = parityBlock.uncles, transactions = blockTxes,
                tx_number = blockTxes.size
        )
    }
}
