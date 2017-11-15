package fund.cyber.index.bitcoin.converter


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.node.model.BitcoinTransaction
import fund.cyber.node.model.BitcoinTransactionIn
import fund.cyber.node.model.BitcoinTransactionOut
import fund.cyber.node.model.BtcdBlock
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Instant


/*---------------Coinbase transaction-----------------------------------------------------*/
val expectedDaoCoinbaseTransactionOut = BitcoinTransactionOut(
        addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = "50",
        asm = "041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84 OP_CHECKSIG",
        out = 0, required_signatures = 1
)

val expectedDaoCoinbaseTransaction = BitcoinTransaction(
        txid = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", size = 135,
        coinbase = "044c86041b020602", fee = "0",
        total_output = "0", total_input = "0", ins = emptyList(),
        block_number = 100000, outs = listOf(expectedDaoCoinbaseTransactionOut),
        block_time = Instant.ofEpochSecond(1293623863).toString(),
        block_hash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
)


/*---------------Regular transaction-----------------------------------------------------*/

val expectedFirstTransactionInput = BitcoinTransactionIn(
        addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = "0.1", asm = "3046",
        tx_id = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03", tx_out = 1
)

val expectedSecondTransactionInput = BitcoinTransactionIn(
        addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = "50", asm = "3046",
        tx_id = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", tx_out = 0
)

val expectedFirstTransactionOut = BitcoinTransactionOut(
        addresses = listOf("1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn"), amount = "5.56",
        asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 0, required_signatures = 1
)

val expectedSecondTransactionOut = BitcoinTransactionOut(
        addresses = listOf("1EYTGtG4LnFfiMvjJdsU7GMGCQvsRSjYhx"), amount = "44.44",
        asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 1, required_signatures = 1
)

val expectedRegularTransaction = BitcoinTransaction(
        txid = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", size = 259,
        fee = "0.10", total_output = "50.00", total_input = "50.1",
        ins = listOf(expectedFirstTransactionInput, expectedSecondTransactionInput),
        outs = listOf(expectedFirstTransactionOut, expectedSecondTransactionOut),
        block_number = 100000, block_time = Instant.ofEpochSecond(1293623863).toString(),
        block_hash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
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
                btcdBlock = btcdBlock, btcdTransaction = btcdBlock.rawtx.first(), inputsByIds = emptyMap()
        )

        Assertions.assertEquals(expectedDaoCoinbaseTransaction, daoCoinbaseTransaction)
    }

    @Test
    @DisplayName("Should convert coinbase transaction")
    fun testConvertRegularTransaction() {


        val daoInputTransactionById = listOf(
                BitcoinTransaction(
                        txid = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03",
                        outs = listOf(
                                BitcoinTransactionOut(
                                        addresses = listOf("2HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), out = 0, amount = "50.1",
                                        asm = "041b0e8c2567c12536aa13357b79a073dc4444", required_signatures = 1
                                ),
                                BitcoinTransactionOut(
                                        addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), out = 1, amount = "0.1",
                                        asm = "041b0e8c2567c12536aa13357b79a073dc4444", required_signatures = 1
                                )
                        ), ins = emptyList(), block_time = "", block_number = 10, fee = "0",
                        total_input = "0", total_output = "0", size = 3,
                        block_hash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506"
                ),
                expectedDaoCoinbaseTransaction
        ).associateBy { tx -> tx.txid }

        val regularTransaction = BitcoinTransactionConverter().btcdTransactionToDao(
                btcdBlock = btcdBlock, btcdTransaction = btcdBlock.rawtx[1], inputsByIds = daoInputTransactionById
        )

        Assertions.assertEquals(expectedRegularTransaction, regularTransaction)
    }
}