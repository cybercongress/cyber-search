package fund.cyber.pump.bitcoin.client


import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import fund.cyber.search.model.bitcoin.*
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigDecimal.ZERO
import java.time.Instant


/*---------------Coinbase transaction-----------------------------------------------------*/
val expectedDaoCoinbaseTxOut = BitcoinTxOut(
    contracts = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = BigDecimal("50"),
    asm = "041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84 OP_CHECKSIG",
    out = 0, requiredSignatures = 1
)

val expectedDaoCoinbaseTx = BitcoinTx(
    hash = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", size = 135,
    coinbase = "044c86041b020602", fee = ZERO,
    totalOutputsAmount = BigDecimal("50"), totalInputsAmount = ZERO, ins = emptyList(),
    blockNumber = 100000, outs = listOf(expectedDaoCoinbaseTxOut),
    blockTime = Instant.ofEpochSecond(1293623863),
    blockHash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506", index = 0
)


/*---------------Regular transaction-----------------------------------------------------*/

val expectedFirstTxInput = BitcoinTxIn(
    contracts = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = BigDecimal("0.1"), asm = "3046",
    txHash = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03", txOut = 1
)

val expectedSecondTxInput = BitcoinTxIn(
    contracts = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J"), amount = BigDecimal("50"), asm = "3046",
    txHash = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87", txOut = 0
)

val expectedFirstTxOut = BitcoinTxOut(
    contracts = listOf("1JqDybm2nWTENrHvMyafbSXXtTk5Uv5QAn"), amount = BigDecimal("5.56"),
    asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 0, requiredSignatures = 1
)

val expectedSecondTxOut = BitcoinTxOut(
    contracts = listOf("1EYTGtG4LnFfiMvjJdsU7GMGCQvsRSjYhx"), amount = BigDecimal("44.44"),
    asm = "OP_DUP OP_HASH160 OP_CHECKSIG", out = 1, requiredSignatures = 1
)

val expectedRegularTx = BitcoinTx(
    hash = "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", size = 259,
    fee = BigDecimal("0.10"), totalOutputsAmount = BigDecimal("50.00"), totalInputsAmount = BigDecimal("50.1"),
    ins = listOf(expectedFirstTxInput, expectedSecondTxInput),
    outs = listOf(expectedFirstTxOut, expectedSecondTxOut),
    blockNumber = 100000, blockTime = Instant.ofEpochSecond(1293623863),
    blockHash = "000000000003ba27aa200b1cecaad478d2b00432346c3f1f3986da1afd33e506", index = 1
)


@DisplayName("Btcd raw transaction to dao transaction conversion test: ")
@Disabled
class BtcdToDaoTxConverterTest {

    private val deserializer = ObjectMapper().registerKotlinModule()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    private val btcdBlockResourceLocation = javaClass.getResource("/converter/bitcoin/raw_block.json")
    private val btcdBlock = deserializer.readValue(btcdBlockResourceLocation, JsonRpcBitcoinBlock::class.java)

    @Test
    @DisplayName("Should convert coinbase transaction")
    fun testConvertCoinbaseTx() {


        val daoCoinbaseTx = JsonRpcToDaoBitcoinTxConverter().convertToDaoTransaction(
            jsonRpcBlock = btcdBlock, jsonRpcTransaction = btcdBlock.tx.first(), inputsByIds = emptyMap(),
            txIndex = 0
        )

        Assertions.assertEquals(expectedDaoCoinbaseTx, daoCoinbaseTx)
    }

    @Test
    @DisplayName("Should convert coinbase transaction")
    fun testConvertRegularTx() {

        val outs = listOf(
            BitcoinCacheTxOutput(
                value = BigDecimal("50.1"), n = 0, addresses = listOf("2HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J")
            ),
            BitcoinCacheTxOutput(
                value = BigDecimal("0.1"), n = 1, addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J")
            )
        )

        val coinbaseOuts = listOf(
            BitcoinCacheTxOutput(
                value = BigDecimal("50"), n = 0, addresses = listOf("1HWqMzw1jfpXb3xyuUZ4uWXY4tqL2cW47J")
            )
        )

        val daoInputTxById = listOf(
            BitcoinCacheTx(
                txid = "83a157f3fd88ac7907c05fc55e271dc4acdc5605d187d646604ca8c0e9382e03",
                outs = outs
            ),
            BitcoinCacheTx(
                txid = "8c14f0db3df150123e6f3dbbf30f8b955a8249b62ac1d1ff16284aefa3d06d87",
                outs = coinbaseOuts
            )
        ).associateBy { tx -> tx.txid }

        val regularTx = JsonRpcToDaoBitcoinTxConverter().convertToDaoTransaction(
            jsonRpcBlock = btcdBlock, jsonRpcTransaction = btcdBlock.tx[1], inputsByIds = daoInputTxById,
            txIndex = 1
        )

        Assertions.assertEquals(expectedRegularTx, regularTx)
    }
}
