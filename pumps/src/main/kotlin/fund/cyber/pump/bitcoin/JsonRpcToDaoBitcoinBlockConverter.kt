package fund.cyber.pump.bitcoin

import fund.cyber.node.common.sumByBigDecimalString
import fund.cyber.node.model.*
import java.math.BigDecimal
import java.time.Instant

class JsonRpcToDaoBitcoinBlockConverter {

    /**
     * @param jsonRpcBlock block to convert
     * @param transactions already converted to dao model transactions for given [jsonRpcBlock]
     * @throws KotlinNullPointerException if [transactions] doesn't contain any transaction from [jsonRpcBlock]
     */
    fun convertToDaoBlock(jsonRpcBlock: JsonRpcBitcoinBlock, transactions: List<BitcoinTransaction>): BitcoinBlock {

        val transactionByHash = transactions.associateBy { tx -> tx.hash }

        val blockTransactionsPreview = jsonRpcBlock.rawtx
                .mapIndexed { index, rpcTx ->
                    val tx = transactionByHash[rpcTx.txid]!!
                    BitcoinBlockTransaction(
                            fee = BigDecimal(tx.fee), hash = tx.hash, block_number = jsonRpcBlock.height,
                            ins = tx.ins.map { input ->
                                BitcoinTransactionPreviewIO(addresses = input.addresses, amount = input.amount)
                            },
                            outs = tx.outs.map { out ->
                                BitcoinTransactionPreviewIO(addresses = out.addresses, amount = out.amount)
                            }, index = index
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
                tx_number = blockTransactionsPreview.size, total_outputs_value = totalOutputsValue.toString(),
                transactionPreviews = blockTransactionsPreview
        )
    }
}