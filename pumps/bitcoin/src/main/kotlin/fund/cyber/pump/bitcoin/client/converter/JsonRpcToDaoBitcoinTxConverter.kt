package fund.cyber.pump.bitcoin.client.converter

import fund.cyber.common.sum
import fund.cyber.common.toSearchHashFormat
import fund.cyber.search.model.bitcoin.BitcoinCacheTxOutput
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import fund.cyber.search.model.bitcoin.CoinbaseTransactionInput
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinBlock
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransaction
import fund.cyber.search.model.bitcoin.JsonRpcBitcoinTransactionOutput
import fund.cyber.search.model.bitcoin.RegularTransactionInput
import org.slf4j.LoggerFactory
import java.math.BigDecimal.ZERO
import java.time.Instant


private val log = LoggerFactory.getLogger(JsonRpcToDaoBitcoinTxConverter::class.java)!!

data class BitcoinBlockInfo(
    val hash: String? = null,
    val time: Instant? = null,
    val number: Long = -1
) {

    constructor(jsonRpcBlock: JsonRpcBitcoinBlock) : this (
        hash = jsonRpcBlock.hash, time = Instant.ofEpochSecond(jsonRpcBlock.time), number = jsonRpcBlock.height
    )
}

/**
 * Bitcoin transaction consists from several "inputs" and "outputs" (for example: 5 inputs, 3 outputs).
 * So in single transaction, you can send tokens to several contracts (several outputs).
 *
 * @see <a href="https://en.bitcoin.it/wiki/Transaction">Bitcoin Official Wiki</a>
 *
 *
 * Default json rpc bitcoin api for transactions has next properties:
 *  * Each output contains information about "value" and "contract" to send bitcoin tokens.
 *  * Each input contains information only about previous transaction hash and output index.
 *
 * So, to get input contract and value, you should find transaction by hash (included in input), get output by index
 *  and read contract and value fields.
 */
class JsonRpcToDaoBitcoinTxConverter {


    /**
     * @param jsonRpcBlock block to obtain transactions to convert
     * @param linkedOutputs should contain all outputs linked from transactions inputs included in [jsonRpcBlock]
     * @throws NullPointerException if [linkedOutputs] doesn't contain output even for one transaction input
     *  from [jsonRpcBlock]
     */
    fun convertToDaoTransactions(
        jsonRpcBlock: JsonRpcBitcoinBlock, linkedOutputs: List<BitcoinCacheTxOutput>): List<BitcoinTx> {

        val outputsByIds = linkedOutputs.associateBy { out -> (out.txid to out.n) }.toMutableMap()

        return jsonRpcBlock.tx
            .mapIndexed { index, btcdTransaction ->
                convertToDaoTransaction(btcdTransaction, outputsByIds, BitcoinBlockInfo(jsonRpcBlock), index)
            }
    }


    /**
     * @param blockInfo block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @param outputsByIds should contain all outputs linked from tx inputs for [jsonRpcTransaction]
     * @throws NullPointerException if [outputsByIds] doesn't contain output even for one transaction input
     */
    fun convertToDaoTransaction(
        jsonRpcTransaction: JsonRpcBitcoinTransaction, outputsByIds: Map<Pair<String, Int>, BitcoinCacheTxOutput>,
        blockInfo: BitcoinBlockInfo, txIndex: Int): BitcoinTx {

        val firstInput = jsonRpcTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return convertToCoinbaseTransaction(jsonRpcTransaction, blockInfo)
        }

        val ins = convertToDaoTransactionInput(jsonRpcTransaction.regularInputs(), outputsByIds)
        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        val totalInput = ins.map { input -> input.amount }.sum()
        val totalOutput = outputs.map { input -> input.amount }.sum()

        return BitcoinTx(
            hash = jsonRpcTransaction.txid.toSearchHashFormat(), blockNumber = blockInfo.number, index = txIndex,
            ins = ins, outs = outputs, totalInputsAmount = totalInput, totalOutputsAmount = totalOutput,
            fee = totalInput - totalOutput, size = jsonRpcTransaction.size,
            blockTime = blockInfo.time, blockHash = blockInfo.hash?.toSearchHashFormat(),
            firstSeenTime = blockInfo.time ?: Instant.now()
        )
    }


    /**
     * @param blockInfo block to obtain transaction's block fields
     * @param jsonRpcTransaction transaction to convert
     * @throws ClassCastException if [jsonRpcTransaction] doesn't contain coinbase input
     */
    fun convertToCoinbaseTransaction(
        jsonRpcTransaction: JsonRpcBitcoinTransaction, blockInfo: BitcoinBlockInfo): BitcoinTx {

        val firstInput = jsonRpcTransaction.vin.first() as CoinbaseTransactionInput

        val outputs = jsonRpcTransaction.vout.map(this::convertToDaoTransactionOutput)

        return BitcoinTx(
            hash = jsonRpcTransaction.txid.toSearchHashFormat(), blockNumber = blockInfo.number, index = 0,
            coinbase = firstInput.coinbase, blockHash = blockInfo.hash?.toSearchHashFormat(),
            blockTime = blockInfo.time, ins = emptyList(), outs = outputs, size = jsonRpcTransaction.size,
            totalInputsAmount = ZERO, totalOutputsAmount = outputs.map { out -> out.amount }.sum(), fee = ZERO,
            firstSeenTime = blockInfo.time ?: Instant.now()
        )
    }


    /**
     * Converts given json rpc transaction inputs to dao transaction inputs.
     * Json rpc input do not contains info about contract and amount, just transaction hash and output number.
     *
     * Dao input contains info about contract and amount. So to fulfill missing fields,
     *   we should use earlier transactions outputs defined by transaction hash and output number.
     *
     * @param txIns json rpc transaction inputs
     * @param outputsByIds should contains all outputs linked from [txIns]
     * @throws NullPointerException if [outputsByIds] doesn't contain even one output linked from [txIns]
     */
    fun convertToDaoTransactionInput(txIns: List<RegularTransactionInput>,
                                     outputsByIds: Map<Pair<String, Int>, BitcoinCacheTxOutput>): List<BitcoinTxIn> {

        return txIns.map { (txid, vout, scriptSig) ->
            log.trace("looking for $txid transaction and output $vout")
            val daoTxOut = outputsByIds[txid to vout]!!
            BitcoinTxIn(
                contracts = daoTxOut.addresses.map { a -> a.toSearchHashFormat() }, amount = daoTxOut.value,
                asm = scriptSig.asm, txHash = txid.toSearchHashFormat(), txOut = vout
            )
        }
    }

    private fun convertToDaoTransactionOutput(jsonRpcTxOut: JsonRpcBitcoinTransactionOutput): BitcoinTxOut {

        return BitcoinTxOut(
            contracts = jsonRpcTxOut.scriptPubKey.addresses.map { a -> a.toSearchHashFormat() },
            amount = jsonRpcTxOut.value, out = jsonRpcTxOut.n, asm = jsonRpcTxOut.scriptPubKey.asm,
            requiredSignatures = jsonRpcTxOut.scriptPubKey.reqSigs
        )
    }
}
