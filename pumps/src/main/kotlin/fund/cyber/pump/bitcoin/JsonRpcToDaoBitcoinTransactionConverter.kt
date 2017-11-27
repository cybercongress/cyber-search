package fund.cyber.pump.bitcoin

import fund.cyber.node.common.sumByBigDecimalString
import fund.cyber.node.model.*
import org.slf4j.LoggerFactory
import java.time.Instant


private val log = LoggerFactory.getLogger(JsonRpcToDaoBitcoinTransactionConverter::class.java)!!

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
class JsonRpcToDaoBitcoinTransactionConverter {


    /**
     * @param jsonRpcBlock block to obtain transactions to convert
     * @param inputs should contains all inputs from transactions included in [jsonRpcBlock]
     * @throws NullPointerException if [inputs] doesn't contain input for any transaction from [jsonRpcBlock]
     */
    fun convertToDaoTransactions(
            jsonRpcBlock: JsonRpcBitcoinBlock, inputs: List<BitcoinTransaction>): List<BitcoinTransaction> {

        val inputsByIds = inputs.associateBy { tx -> tx.txid }.toMutableMap()

        return jsonRpcBlock.rawtx
                .map { btcdTransaction ->
                    val daoTx = convertToDaoTransaction(btcdTransaction, inputsByIds, jsonRpcBlock)
                    //input tx can be from same block
                    inputsByIds.put(daoTx.txid, daoTx)
                    daoTx
                }
    }


    /**
     * @param jsonRpcBlock block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @param inputsByIds should contains all inputs for [jsonRpcTransaction]
     * @throws NullPointerException if [inputsByIds] doesn't contain any input for [jsonRpcTransaction]
     */
    fun convertToDaoTransaction(
            jsonRpcTransaction: JsonRpcBitcoinTransaction, inputsByIds: Map<String, BitcoinTransaction>,
            jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTransaction {

        val firstInput = jsonRpcTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return convertToCoinbaseTransaction(jsonRpcTransaction, jsonRpcBlock)
        }

        val ins = convertToDaoTransactionInput(jsonRpcTransaction.regularInputs(), inputsByIds)
        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        val totalInput = ins.sumByBigDecimalString { input -> input.amount }
        val totalOutput = outputs.sumByBigDecimalString { out -> out.amount }

        return BitcoinTransaction(
                txid = jsonRpcTransaction.txid, block_number = jsonRpcBlock.height,
                ins = ins, outs = outputs, total_input = totalInput.toString(), total_output = totalOutput.toString(),
                fee = (totalInput - totalOutput).toString(), size = jsonRpcTransaction.size,
                block_time = Instant.ofEpochSecond(jsonRpcBlock.time),
                block_hash = jsonRpcBlock.hash
        )
    }


    /**
     * @param jsonRpcBlock block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @throws ClassCastException if [jsonRpcTransaction] doesn't contain coinbase input
     */
    fun convertToCoinbaseTransaction(
            jsonRpcTransaction: JsonRpcBitcoinTransaction, jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTransaction {

        val firstInput = jsonRpcTransaction.vin.first() as CoinbaseTransactionInput

        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        return BitcoinTransaction(
                txid = jsonRpcTransaction.txid, block_number = jsonRpcBlock.height,
                coinbase = firstInput.coinbase, fee = "0", block_hash = jsonRpcBlock.hash,
                block_time = Instant.ofEpochSecond(jsonRpcBlock.time),
                ins = emptyList(), total_input = "0",
                outs = outputs, total_output = "0", size = jsonRpcTransaction.size
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
                                     inputsByIds: Map<String, BitcoinTransaction>): List<BitcoinTransactionIn> {

        return txIns.map { (txid, vout, scriptSig) ->
            log.trace("looking for $txid transaction and output $vout")
            val daoTxOut = inputsByIds[txid]!!.getOutputByNumber(vout)
            BitcoinTransactionIn(
                    addresses = daoTxOut.addresses, amount = daoTxOut.amount, asm = scriptSig.asm, tx_id = txid, tx_out = vout
            )
        }
    }

    private fun convertToDaoTransactionOutput(jsonRpcTxOut: JsonRpcBitcoinTransactionOutput): BitcoinTransactionOut {

        return BitcoinTransactionOut(
                addresses = jsonRpcTxOut.scriptPubKey.addresses,
                amount = jsonRpcTxOut.value, out = jsonRpcTxOut.n, asm = jsonRpcTxOut.scriptPubKey.asm,
                required_signatures = jsonRpcTxOut.scriptPubKey.reqSigs
        )
    }
}