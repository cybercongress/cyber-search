package fund.cyber.index.bitcoin.converter


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.index.btcd.*
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionOut
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


val expectedDaoCoinbaseTransactionOut = BitcoinTransactionOut(
        address = "1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", amount = BigDecimal("50"),
        asm = "041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84 OP_CHECKSIG",
        out = 0, required_signatures = 1)

val expectedDaoCoinbaseTransaction = BitcoinTransaction(
        txId = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", size = 135,
        coinbase = "044c86041b020602", fee = BigDecimal.ZERO, lock_time = BigInteger.ZERO,
        total_output = BigDecimal.ZERO, total_input = BigDecimal.ZERO, ins = emptyList(),
        block_number = BigInteger("100000"), outs = listOf(expectedDaoCoinbaseTransactionOut),
        blockTime = Instant.ofEpochSecond(1293623863)
)


@DisplayName("Btcd raw transaction to dao transaction conversion test: ")
class BtcdToDaoTransactionConverterTest {

    val deserializer = ObjectMapper().registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    val btcdBlockResourceLocation = javaClass.getResource("/converter/bitcoin/raw_block.json")
    val btcdBlock = deserializer.readValue(btcdBlockResourceLocation, BtcdBlock::class.java)


    @Test
    @DisplayName("Should convert coinbase transaction")
    fun testParseBlock() {

        val daoCoinbaseTransaction = BitcoinTransactionConverter().btcdTransactionToDao(
                btcdBlock = btcdBlock, btcdTransaction = btcdBlock.rawtx.first(),
                inputDaoTransactionById = emptyMap()
        )

        Assertions.assertEquals(expectedDaoCoinbaseTransaction, daoCoinbaseTransaction)
    }
}