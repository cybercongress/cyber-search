package fund.cyber.index.bitcoin.converter

import fund.cyber.index.btcd.*
import fund.cyber.node.common.sumByBigDecimal
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import org.slf4j.LoggerFactory
import java.time.Instant

val log = LoggerFactory.getLogger(BitcoinTransactionConverter::class.java)!!


class BitcoinTransactionConverter {

    fun btcdTransactionsToDao(btcdBlock: BtcdBlock, inputs: List<BitcoinTransaction>): List<BitcoinTransaction> {

        val inputsByIds = inputs.associateBy { tx -> tx.txid }.toMutableMap()

        return btcdBlock.rawtx
                .map { btcdTransaction ->
                    //input tx can be from same block
                    val daoTx = btcdTransactionToDao(btcdTransaction, inputsByIds, btcdBlock)
                    inputsByIds.put(daoTx.txid, daoTx)
                    daoTx
                }
    }


    fun btcdTransactionToDao(btcdTransaction: BtcdTransaction,
                             inputsByIds: Map<String, BitcoinTransaction>, btcdBlock: BtcdBlock): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first()

        // coinbase case
        if (firstInput is CoinbaseTransactionInput) {
            return btcdCoinbaseTxToDao(btcdTransaction, btcdBlock)
        }

        val ins = btcdTxInToDao(btcdTransaction.regularInputs(), inputsByIds)
        val outputs = btcdTransaction.vout.map(this::btcdTxOutToDao)

        val totalInput = ins.sumByBigDecimal { input -> input.amount }
        val totalOutput = outputs.sumByBigDecimal { out -> out.amount }

        return BitcoinTransaction(
                txid = btcdTransaction.txid, block_number = btcdBlock.height,
                ins = ins, outs = outputs, total_input = totalInput.toString(), total_output = totalOutput.toString(),
                fee = (totalInput - totalOutput).toString(), size = btcdTransaction.size,
                block_time = Instant.ofEpochSecond(btcdBlock.time).toString(),
                block_hash = btcdBlock.hash
        )
    }


    fun btcdCoinbaseTxToDao(btcdTransaction: BtcdTransaction, btcdBlock: BtcdBlock): BitcoinTransaction {

        val firstInput = btcdTransaction.vin.first() as CoinbaseTransactionInput

        val outputs = btcdTransaction.vout.map(this::btcdTxOutToDao)

        return BitcoinTransaction(
                txid = btcdTransaction.txid, block_number = btcdBlock.height,
                coinbase = firstInput.coinbase, fee = "0", block_hash = btcdBlock.hash,
                block_time = Instant.ofEpochSecond(btcdBlock.time).toString(),
                ins = emptyList(), total_input = "0",
                outs = outputs, total_output = "0", size = btcdTransaction.size
        )
    }


    /**
     * Converts given btcd tx inputs to dao tx inputs.
     * Btcd input do not contains info about address and amount, just txid and n.
     * Dao input contains info about address and amount. So to fulfill missing fields, we should use earlier transactions
     *   outputs defined by txid and n.
     *
     * @param btcdTxIns current transaction inputs
     */
    fun btcdTxInToDao(btcdTxIns: List<BtcdRegularTransactionInput>,
                      inputsByIds: Map<String, BitcoinTransaction>): List<BitcoinTransactionIn> {

        return btcdTxIns.map { (txid, vout, scriptSig) ->
            log.debug("looking for $txid transaction and output $vout")
            val daoTxOut = inputsByIds[txid]!!.getOutputByNumber(vout)
            BitcoinTransactionIn(
                    addresses = daoTxOut.addresses, amount = daoTxOut.amount, asm = scriptSig.asm, tx_id = txid, tx_out = vout
            )
        }
    }

    private fun btcdTxOutToDao(btcdTxOut: BtcdTransactionOutput): BitcoinTransactionOut {

        return BitcoinTransactionOut(
                addresses = btcdTxOut.scriptPubKey.addresses,
                amount = btcdTxOut.value, out = btcdTxOut.n, asm = btcdTxOut.scriptPubKey.asm,
                required_signatures = btcdTxOut.scriptPubKey.reqSigs
        )
    }
}