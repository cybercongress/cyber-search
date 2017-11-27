package fund.cyber.index.bitcoin.converter

import fund.cyber.node.common.sumByBigDecimalString
import fund.cyber.node.model.*
import org.slf4j.LoggerFactory
import java.time.Instant

private val log = LoggerFactory.getLogger(BitcoinTransactionConverter::class.java)!!


class BitcoinTransactionConverter {

    fun btcdTransactionsToDao(jsonRpcBlock: JsonRpcBitcoinBlock, inputs: List<BitcoinTransaction>): List<BitcoinTransaction> {

        val inputsByIds = inputs.associateBy { tx -> tx.txid }.toMutableMap()

        return jsonRpcBlock.rawtx
                .map { btcdTransaction ->
                    val daoTx = btcdTransactionToDao(btcdTransaction, inputsByIds, jsonRpcBlock)
                    //input tx can be from same block
                    inputsByIds.put(daoTx.txid, daoTx)
                    daoTx
                }
    }


    fun btcdTransactionToDao(jsonRpcTransaction: JsonRpcBitcoinTransaction,
                             inputsByIds: Map<String, BitcoinTransaction>, jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTransaction {

        val firstInput = jsonRpcTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return btcdCoinbaseTxToDao(jsonRpcTransaction, jsonRpcBlock)
        }

        val ins = btcdTxInToDao(jsonRpcTransaction.regularInputs(), inputsByIds)
        val outputs = jsonRpcTransaction.vout.map(this::btcdTxOutToDao)

        val totalInput = ins.sumByBigDecimalString { input -> input.amount }
        val totalOutput = outputs.sumByBigDecimalString { out -> out.amount }

        return BitcoinTransaction(
                txid = jsonRpcTransaction.txid, block_number = jsonRpcBlock.height,
                ins = ins, outs = outputs, total_input = totalInput.toString(), total_output = totalOutput.toString(),
                fee = (totalInput - totalOutput).toString(), size = jsonRpcTransaction.size,
                block_time = Instant.ofEpochSecond(jsonRpcBlock.time).toString(),
                block_hash = jsonRpcBlock.hash
        )
    }


    fun btcdCoinbaseTxToDao(jsonRpcTransaction: JsonRpcBitcoinTransaction, jsonRpcBlock: JsonRpcBitcoinBlock): BitcoinTransaction {

        val firstInput = jsonRpcTransaction.vin.first() as CoinbaseTransactionInput

        val outputs = jsonRpcTransaction.vout.map(this::btcdTxOutToDao)

        return BitcoinTransaction(
                txid = jsonRpcTransaction.txid, block_number = jsonRpcBlock.height,
                coinbase = firstInput.coinbase, fee = "0", block_hash = jsonRpcBlock.hash,
                block_time = Instant.ofEpochSecond(jsonRpcBlock.time).toString(),
                ins = emptyList(), total_input = "0",
                outs = outputs, total_output = "0", size = jsonRpcTransaction.size
        )
    }


    /**
     * Converts given btcd tx inputs to dao tx inputs.
     * Btcd input do not contains info about address and amount, just txid and n.
     * Dao input contains info about address and amount. So to fulfill missing fields, we should use earlier transactions
     *   outputs defined by txid and n.
     *
     * @param txIns current transaction inputs
     */
    fun btcdTxInToDao(txIns: List<RegularTransactionInput>,
                      inputsByIds: Map<String, BitcoinTransaction>): List<BitcoinTransactionIn> {

        return txIns.map { (txid, vout, scriptSig) ->
            log.trace("looking for $txid transaction and output $vout")
            val daoTxOut = inputsByIds[txid]!!.getOutputByNumber(vout)
            BitcoinTransactionIn(
                    addresses = daoTxOut.addresses, amount = daoTxOut.amount, asm = scriptSig.asm, tx_id = txid, tx_out = vout
            )
        }
    }

    private fun btcdTxOutToDao(jsonRpcTxOut: JsonRpcBitcoinTransactionOutput): BitcoinTransactionOut {

        return BitcoinTransactionOut(
                addresses = jsonRpcTxOut.scriptPubKey.addresses,
                amount = jsonRpcTxOut.value, out = jsonRpcTxOut.n, asm = jsonRpcTxOut.scriptPubKey.asm,
                required_signatures = jsonRpcTxOut.scriptPubKey.reqSigs
        )
    }
}