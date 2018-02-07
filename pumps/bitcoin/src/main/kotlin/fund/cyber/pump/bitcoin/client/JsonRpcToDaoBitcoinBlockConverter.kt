package fund.cyber.pump.bitcoin.client

import fund.cyber.search.common.sum
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import java.time.Instant


class JsonRpcToDaoBitcoinBlockConverter {

    /**
     * @param jsonRpcBlock block to convert
     * @param transactions already converted to dao model transactions for given [jsonRpcBlock]
     * @throws KotlinNullPointerException if [transactions] doesn't contain any transaction from [jsonRpcBlock]
     */
    fun convertToDaoBlock(jsonRpcBlock: JsonRpcBitcoinBlock, transactions: List<BitcoinTx>): BitcoinBlock {

        val totalOutputsValue = transactions.flatMap { tx -> tx.outs }.map { out -> out.amount }.sum()

        return BitcoinBlock(
                hash = jsonRpcBlock.hash, size = jsonRpcBlock.size, version = jsonRpcBlock.version, bits = jsonRpcBlock.bits,
                difficulty = jsonRpcBlock.difficulty.toBigInteger(), nonce = jsonRpcBlock.nonce,
                time = Instant.ofEpochSecond(jsonRpcBlock.time), weight = jsonRpcBlock.weight,
                merkleroot = jsonRpcBlock.merkleroot, height = jsonRpcBlock.height,
                txNumber = jsonRpcBlock.tx.size, totalOutputsAmount = totalOutputsValue
        )
    }
}