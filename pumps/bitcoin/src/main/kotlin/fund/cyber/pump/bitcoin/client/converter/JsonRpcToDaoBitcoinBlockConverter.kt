package fund.cyber.pump.bitcoin.client.converter

import fund.cyber.common.sum
import fund.cyber.common.toSearchHashFormat
import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.getBlockReward
import java.time.Instant


class JsonRpcToDaoBitcoinBlockConverter {

    /**
     * @param jsonRpcBlock block to convert
     * @param transactions already converted to dao model transactions for given [jsonRpcBlock]
     * @throws KotlinNullPointerException if [transactions] doesn't contain any transaction from [jsonRpcBlock]
     */
    fun convertToDaoBlock(jsonRpcBlock: JsonRpcBitcoinBlock, transactions: List<BitcoinTx>): BitcoinBlock {

        val totalOutputsValue = transactions.flatMap { tx -> tx.outs }.map { out -> out.amount }.sum()

        val coinbaseTx = transactions.firstOrNull()
        val coinbaseTxMinerOutput = coinbaseTx?.outs?.firstOrNull()

        return BitcoinBlock(
            hash = jsonRpcBlock.hash.toSearchHashFormat(),
            parentHash = jsonRpcBlock.previousblockhash?.toSearchHashFormat() ?: "",
            size = jsonRpcBlock.size,
            minerContractHash = coinbaseTxMinerOutput?.contracts?.first()?.toSearchHashFormat() ?: "",
            version = jsonRpcBlock.version, blockReward = getBlockReward(jsonRpcBlock.height),
            txFees = transactions.map { tx -> tx.fee }.sum(), coinbaseData = coinbaseTx?.coinbase ?: "",
            bits = jsonRpcBlock.bits, difficulty = jsonRpcBlock.difficulty.toBigInteger(),
            nonce = jsonRpcBlock.nonce, time = Instant.ofEpochSecond(jsonRpcBlock.time),
            weight = jsonRpcBlock.weight, merkleroot = jsonRpcBlock.merkleroot.toSearchHashFormat(),
            height = jsonRpcBlock.height,
            txNumber = jsonRpcBlock.tx.size, totalOutputsAmount = totalOutputsValue
        )
    }
}
