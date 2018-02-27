package fund.cyber.pump.bitcoin.client

import fund.cyber.common.sum
import fund.cyber.search.model.bitcoin.*
import org.slf4j.LoggerFactory
import java.math.BigDecimal.ZERO
import java.time.Instant


private val log = LoggerFactory.getLogger(JsonRpcToDaoBitcoinTxConverter::class.java)!!

/**
 * Bitcoin transaction consists from several "inputs" and "outputs" (for example: 5 inputs, 3 outputs).
 * So in single transaction, you can send tokens to several addresses (several outputs).
 *
 * @see <a href="https://en.bitcoin.it/wiki/Transaction">Bitcoin Official Wiki</a>
 *
 *
 * Default json rpc bitcoin api for transactions has next properties:
 *  * Each output contains information about "value" and "address" to send bitcoin tokens.
 *  * Each input contains information only about previous transaction hash and output index.
 *
 * So, to get input address and value, you should find transaction by hash (included in input), get output by index
 *  and read address and value fields.
 */
class JsonRpcToDaoBitcoinTxConverter {


    /**
     * @param jsonRpcBlock block to obtain transactions to convert
     * @param inputs should contains all inputs from transactions included in [jsonRpcBlock]
     * @throws NullPointerException if [inputs] doesn't contain input for any transaction from [jsonRpcBlock]
     */
    fun convertToDaoTransactions(
            jsonRpcBlock: JsonRpcBitcoinBlock, inputs: List<JsonRpcBitcoinTransaction>): List<BitcoinTx> {

        val inputsByIds = inputs.associateBy { tx -> tx.txid }.toMutableMap()

        return jsonRpcBlock.rawtx
                .map { btcdTransaction ->
                    convertToDaoTransaction(btcdTransaction, inputsByIds, jsonRpcBlock)
                }
    }


    /**
     * @param jsonRpcBlock block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @param inputsByIds should contains all inputs for [jsonRpcTransaction]
     * @throws NullPointerException if [inputsByIds] doesn't contain any input for [jsonRpcTransaction]
     */
    fun convertToDaoTransaction(
            jsonRpcTransaction: JsonRpcBitcoinTransaction, inputsByIds: Map<String, JsonRpcBitcoinTransaction>,
            jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTx {

        val firstInput = jsonRpcTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return convertToCoinbaseTransaction(jsonRpcTransaction, jsonRpcBlock)
        }

        val ins = convertToDaoTransactionInput(jsonRpcTransaction.regularInputs(), inputsByIds)
        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        val totalInput = ins.map { input -> input.amount }.sum()
        val totalOutput = outputs.map { input -> input.amount }.sum()

        return BitcoinTx(
                hash = jsonRpcTransaction.txid, blockNumber = jsonRpcBlock.height,
                ins = ins, outs = outputs, totalInputsAmount = totalInput, totalOutputsAmount = totalOutput,
                fee = totalInput - totalOutput, size = jsonRpcTransaction.size,
                blockTime = Instant.ofEpochSecond(jsonRpcBlock.time), blockHash = jsonRpcBlock.hash
        )
    }


    /**
     * @param jsonRpcBlock block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @throws ClassCastException if [jsonRpcTransaction] doesn't contain coinbase input
     */
    fun convertToCoinbaseTransaction(
            jsonRpcTransaction: JsonRpcBitcoinTransaction, jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTx {

        val firstInput = jsonRpcTransaction.vin.first() as CoinbaseTransactionInput

        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        return BitcoinTx(
                hash = jsonRpcTransaction.txid, blockNumber = jsonRpcBlock.height,
                coinbase = firstInput.coinbase, blockHash = jsonRpcBlock.hash,
                blockTime = Instant.ofEpochSecond(jsonRpcBlock.time),
                ins = emptyList(), outs = outputs, size = jsonRpcTransaction.size,
                totalInputsAmount = ZERO, totalOutputsAmount = outputs.map { out -> out.amount }.sum(), fee = ZERO
        )
    }


    /**
     * Converts given json rpc transaction inputs to dao transaction inputs.
     * Json rpc input do not contains info about address and amount, just transaction hash and output number.
     *
     * Dao input contains info about address and amount. So to fulfill missing fields,
     *   we should use earlier transactions outputs defined by transaction hash and output number.
     *
     * @param txIns json rpc transaction inputs
     * @param inputsByIds should contains all inputs define in [txIns]
     * @throws NullPointerException if [inputsByIds] doesn't contain any input defined in [txIns]
     */
    fun convertToDaoTransactionInput(txIns: List<RegularTransactionInput>,
                                     inputsByIds: Map<String, JsonRpcBitcoinTransaction>): List<BitcoinTxIn> {

        return txIns.map { (txid, vout, scriptSig) ->
            log.trace("looking for $txid transaction and output $vout")
            val daoTxOut = inputsByIds[txid]!!.getOutputByNumber(vout)
            BitcoinTxIn(
                    addresses = daoTxOut.scriptPubKey.addresses, amount = daoTxOut.value,
                    asm = scriptSig.asm, txHash = txid, txOut = vout
            )
        }
    }

    private fun convertToDaoTransactionOutput(jsonRpcTxOut: JsonRpcBitcoinTransactionOutput): BitcoinTxOut {

        return BitcoinTxOut(
                addresses = jsonRpcTxOut.scriptPubKey.addresses,
                amount = jsonRpcTxOut.value, out = jsonRpcTxOut.n, asm = jsonRpcTxOut.scriptPubKey.asm,
                requiredSignatures = jsonRpcTxOut.scriptPubKey.reqSigs
        )
    }
}