package fund.cyber.index.bitcoin.converter

import fund.cyber.node.common.sumByBigDecimalString
import fund.cyber.node.model.*
import java.time.Instant

class BitcoinBlockConverter {

    fun btcdBlockToDao(jsonRpcBlock: JsonRpcBitcoinBlock, transactions: List<BitcoinTransaction>): BitcoinBlock {

        val blockTransactionsPreview = transactions
                .map { tx ->
                    BitcoinBlockTransaction(
                            fee = tx.fee, hash = tx.txid, block_number = jsonRpcBlock.height,
                            ins = tx.ins.map { input ->
                                BitcoinTransactionPreviewIO(addresses = input.addresses, amount = input.amount)
                            },
                            outs = tx.outs.map { out ->
                                BitcoinTransactionPreviewIO(addresses = out.addresses, amount = out.amount)
                            }
                    )
                }

        val totalOutputsValue = blockTransactionsPreview
                .flatMap { tx -> tx.outs }
                .sumByBigDecimalString { out -> out.amount }

        return BitcoinBlock(
                hash = jsonRpcBlock.hash, size = jsonRpcBlock.size, version = jsonRpcBlock.version, bits = jsonRpcBlock.bits,
                difficulty = jsonRpcBlock.difficulty.toBigInteger(), nonce = jsonRpcBlock.nonce,
                time = Instant.ofEpochSecond(jsonRpcBlock.time), weight = jsonRpcBlock.weight,
                merkleroot = jsonRpcBlock.merkleroot, height = jsonRpcBlock.height,
                tx_number = blockTransactionsPreview.size, total_outputs_value = totalOutputsValue.toString()
        )
    }
}