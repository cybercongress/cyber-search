package cyber.search.model.bitcoin

import java.math.BigDecimal
import java.time.Instant

data class BitcoinAddress(
        val id: String,
        val confirmedBalance: BigDecimal,
        val confirmedTotalReceived: BigDecimal,
        val confirmedTxNumber: Int,
        val unconfirmedTxAmounts: Map<String, BigDecimal> = emptyMap()
) : BitcoinItem


data class BitcoinAddressTx(
        val address: String,
        val blockTime: Instant,
        val blockNumber: Long,
        val hash: String,
        val fee: BigDecimal,
        val ins: List<BitcoinTxPreviewIO>,
        val outs: List<BitcoinTxPreviewIO>
) : BitcoinItem