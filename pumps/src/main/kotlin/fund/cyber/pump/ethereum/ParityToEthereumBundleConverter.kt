package fund.cyber.pump.ethereum

import fund.cyber.node.common.Chain
import fund.cyber.node.common.hexToLong
import fund.cyber.node.common.sum
import fund.cyber.node.model.*
import org.web3j.protocol.core.methods.response.EthBlock
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant

class ParityToEthereumBundleConverter(private val chain: Chain) {

    private val weiToEthRate = BigDecimal("1E-18")

    fun convertToBundle(parityBlock: EthBlock.Block, uncles: List<EthBlock.Block>): EthereumBlockBundle {

        val block = parityBlockToDao(parityBlock)
        val blockUncles = parityUnclesToDao(block, uncles)
        val addressUncles = blockUncles.map { uncle -> EthereumAddressUncle(uncle) }
        val addressBlock = EthereumAddressMinedBlock(block)

        val transactions = parityTransactionsToDao(parityBlock)
        val addressTxesPreviews = toAddressTxesPreviews(transactions)
        val blockTxesPreviews = transactions.mapIndexed { index, tx -> EthereumBlockTxPreview(tx, index) }

        return EthereumBlockBundle(
                chain = chain, hash = parityBlock.hash, parentHash = parityBlock.parentHash ?: "-1",
                number = parityBlock.number.toLong(),
                block = block, uncles = blockUncles, addressBlock = addressBlock, addressUncles = addressUncles,
                transactions = transactions, addressTxesPreviews = addressTxesPreviews,
                blockTxesPreviews = blockTxesPreviews

        )
    }

    private fun parityUnclesToDao(block: EthereumBlock, uncles: List<EthBlock.Block>): List<EthereumUncle> {
        return uncles.mapIndexed { index, uncle ->
            val uncleNumber = uncle.number.toLong()
            EthereumUncle(
                    miner = uncle.miner, hash = uncle.hash, number = uncleNumber, position = index,
                    timestamp = Instant.ofEpochSecond(uncle.timestampRaw.hexToLong()),
                    block_number = block.number, block_time = block.timestamp, block_hash = block.hash,
                    uncle_reward = getUncleReward(uncleNumber, block.number, BigDecimal(block.block_reward)).toString()
            )
        }
    }

    private fun toAddressTxesPreviews(transactions: List<EthereumTransaction>): List<EthereumAddressTxPreview> {
        return transactions.flatMap { tx ->
            tx.addressesUsedInTransaction().map { address -> EthereumAddressTxPreview(tx, address) }
        }
    }

    private fun parityTransactionsToDao(parityBlock: EthBlock.Block): List<EthereumTransaction> {

        return parityBlock.transactions
                .filterIsInstance<EthBlock.TransactionObject>()
                .map { parityTx ->
                    EthereumTransaction(
                            from = parityTx.from, to = parityTx.to, nonce = parityTx.nonce.toLong(),
                            value = (BigDecimal(parityTx.value) * weiToEthRate).toString(),
                            hash = parityTx.hash, block_hash = parityBlock.hash,
                            block_number = parityBlock.numberRaw.hexToLong(),
                            block_time = Instant.ofEpochSecond(parityBlock.timestampRaw.hexToLong()),
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
                tx_fees = blockTxesFees.sum().toString(), block_reward = blockReward.toString(),
                uncles_reward = uncleReward.toString()
        )
    }
}

