package fund.cyber.search.model.ethereum

import java.math.BigDecimal
import java.time.Instant

val weiToEthRate = BigDecimal("1E-18")

data class EthereumTx(
        val hash: String,
        val nonce: Long,                //parsed from hex
        val blockHash: String?,        //null when its pending
        val blockNumber: Long,         //parsed from hex   //null when its pending
        val blockTime: Instant,
        val positionInBlock: Int,       //txes from one block ordering field
        val from: String,
        val to: String?,                //null when its a contract creation transaction.
        val value: BigDecimal,          //decimal   //parsed from hex
        val gasPrice: BigDecimal,      //parsed from hex
        val gasLimit: Long,            //parsed from hex
        val gasUsed: Long,             //parsed from hex
        val fee: BigDecimal,            //decimal //calculated
        val input: String,
        val createdSmartContract: String?            //creates contract hash
) {
    fun addressesUsedInTransaction() = listOfNotNull(from, to, createdSmartContract)
            .filter { address -> !address.isEmpty() }
}
