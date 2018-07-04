package fund.cyber.pump.bitcoin.client.converter

import fund.cyber.common.toSearchHashFormat
import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import org.springframework.stereotype.Component


fun Pair<String, Int>.txOutputCacheKey(): String = "${first}__$second"

@Component
class JsonRpcBlockToBitcoinBundleConverter(
    private val outputsStorage: BitcoinTxOutputsStorage
) {

    private val transactionConverter = JsonRpcToDaoBitcoinTxConverter()
    private val blockConverter = JsonRpcToDaoBitcoinBlockConverter()


    fun convertToBundle(jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinBlockBundle {

        outputsStorage.updateCache(jsonRpcBlock)

        val linkedOutputs = outputsStorage.getLinkedOutputsByBlock(jsonRpcBlock)
        val transactions = transactionConverter.convertToDaoTransactions(jsonRpcBlock, linkedOutputs)
        val block = blockConverter.convertToDaoBlock(jsonRpcBlock, transactions)

        return BitcoinBlockBundle(
            hash = jsonRpcBlock.hash.toSearchHashFormat(),
            parentHash = jsonRpcBlock.previousblockhash?.toSearchHashFormat() ?: "-1",
            number = jsonRpcBlock.height, block = block, transactions = transactions,
            blockSize = jsonRpcBlock.size
        )
    }

    fun convertToMempoolTx(jsonRpcTx: JsonRpcBitcoinTransaction): BitcoinTx {

        val linkedOutputs = outputsStorage.getLinkedOutputsByTx(jsonRpcTx)

        val outputsByIds = linkedOutputs.associateBy { out -> (out.txid to out.n) }.toMutableMap()

        return transactionConverter.convertToDaoTransaction(jsonRpcTx, outputsByIds, BitcoinBlockInfo(), -1)
    }

}
