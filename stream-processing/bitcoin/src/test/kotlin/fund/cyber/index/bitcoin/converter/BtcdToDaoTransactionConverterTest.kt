package fund.cyber.index.bitcoin.converter


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.index.btcd.*
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant


/*---------------Coinbase transaction-----------------------------------------------------*/
val expectedDaoCoinbaseTransactionOut = BitcoinTransactionOut(
        address = "1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", amount = BigDecimal("50"),
        asm = "041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84 OP_CHECKSIG",
        out = 0, required_signatures = 1
)

val expectedDaoCoinbaseTransaction = BitcoinTransaction(
        txId = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", size = 135,
        coinbase = "044c86041b020602", fee = BigDecimal.ZERO, lock_time = BigInteger.ZERO,
        total_output = BigDecimal.ZERO, total_input = BigDecimal.ZERO, ins = emptyList(),
        block_number = BigInteger("100000"), outs = listOf(expectedDaoCoinbaseTransactionOut),
        blockTime = Instant.ofEpochSecond(1293623863)
)


/*---------------Regular transaction-----------------------------------------------------*/

val expectedFirstTransactionInput = BitcoinTransactionIn(
        address = "1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", amount = BigDecimal("0.1"), asm = "3046",
        tx_id = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03", tx_out = 1
)

val expectedSecondTransactionInput = BitcoinTransactionIn(
        address = "1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", amount = BigDecimal("50"), asm = "3046",
        tx_id = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", tx_out = 0
)

val expectedFirstTransactionOut = BitcoinTransactionOut(
        address = "1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn", amount = BigDecimal("5.56"),
        asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 0, required_signatures = 1
)

val expectedSecondTransactionOut = BitcoinTransactionOut(
        address = "1EYTGtG4LnFfiMvjJdsU7GMGCQvsRSjYhx", amount = BigDecimal("44.44"),
        asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 1, required_signatures = 1
)

val expectedRegularTransaction = BitcoinTransaction(
        txId = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", size = 259,
        fee = BigDecimal("0.10"), lock_time = BigInteger.ZERO,
        total_output = BigDecimal("50.00"), total_input = BigDecimal("50.1"),
        ins = listOf(expectedFirstTransactionInput, expectedSecondTransactionInput),
        outs = listOf(expectedFirstTransactionOut, expectedSecondTransactionOut),
        block_number = BigInteger("100000"),
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
    fun testConvertCoinbaseTransaction() {

        val daoCoinbaseTransaction = BitcoinTransactionConverter().btcdTransactionToDao(
                btcdBlock = btcdBlock, btcdTransaction = btcdBlock.rawtx.first(),
                inputDaoTransactionById = emptyMap()
        )

        Assertions.assertEquals(expectedDaoCoinbaseTransaction, daoCoinbaseTransaction)
    }

    @Test
    @DisplayName("Should convert coinbase transaction")
    fun testConvertRegularTransaction() {

        val daoInputTransactionById = listOf(
                BitcoinTransaction(
                        txId = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03",
                        outs = listOf(
                                BitcoinTransactionOut(
                                        address = "2HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", out = 0, amount = BigDecimal("50.1"),
                                        asm = "041b0e8c2567c12536aa13357b79a073dc4444", required_signatures = 1
                                ),
                                BitcoinTransactionOut(
                                        address = "1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J", out = 1, amount = BigDecimal("0.1"),
                                        asm = "041b0e8c2567c12536aa13357b79a073dc4444", required_signatures = 1
                                )
                        ), ins = emptyList(), blockTime = Instant.now(), block_number = BigInteger.TEN, fee = BigDecimal.ZERO,
                        total_input = BigDecimal.ZERO, total_output = BigDecimal.ZERO, size = 3, lock_time = BigInteger.TEN
                ),
                expectedDaoCoinbaseTransaction
        ).associateBy { tx -> tx.txId }

        val regularTransaction = BitcoinTransactionConverter().btcdTransactionToDao(
                btcdBlock = btcdBlock, btcdTransaction = btcdBlock.rawtx[1],
                inputDaoTransactionById = daoInputTransactionById
        )

        Assertions.assertEquals(expectedRegularTransaction, regularTransaction)
    }
}