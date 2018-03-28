package fund.cyber.pump.bitcoin.client.genesis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.genesis.GenesisDataProvider
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.bitcoin.BitcoinTxIn
import fund.cyber.search.model.bitcoin.BitcoinTxOut
import fund.cyber.search.model.chains.Chain
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Instant

interface BitcoinGenesisDataProvider : GenesisDataProvider<BitcoinBlockBundle>

@Component
class BitcoinGenesisDataFileProvider(
        private val genesisFileRootDirectory: String = "genesis",
        private val chain: Chain
) : BitcoinGenesisDataProvider {

    override fun provide(blockBundle: BitcoinBlockBundle): BitcoinBlockBundle {

        val filePath = "/$genesisFileRootDirectory/${chain.lowerCaseName}.json"

        val jkMapper = ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(KotlinModule())

        val genesis: BitcoinGenesisFile = jkMapper
                .readValue(
                        BitcoinGenesisDataFileProvider::class.java.getResourceAsStream(filePath),
                        BitcoinGenesisFile::class.java
                )

        val transactions = genesis.transactions.map {
            val ins = it.ins.map {
                BitcoinTxIn(
                        addresses = it.addresses, amount = it.amount,
                        asm = it.asm, txHash = it.txHash, txOut = it.txOut
                )
            }

            val outs = it.outs.map {
                BitcoinTxOut(
                        addresses = it.addresses, amount = it.amount, asm = "",
                        out = 0, requiredSignatures = 0
                )
            }

            BitcoinTx(
                    hash = it.hash, blockNumber =  it.blockNumber, blockHash = it.blockHash,
                    coinbase =  it.coinbase, blockTime = Instant.ofEpochSecond(it.blockTime),
                    size = it.size, fee = it.fee, totalInputsAmount = it.totalInputsAmount,
                    totalOutputsAmount = it.totalOutputsAmount, ins = ins, outs = outs
            )
        }

        return BitcoinBlockBundle(
                hash = blockBundle.hash, parentHash = blockBundle.parentHash, blockSize = blockBundle.blockSize,
                transactions = transactions, block = blockBundle.block, number = blockBundle.number
        )
    }

}

class BitcoinGenesisFile(
        val transactions: List<Tx>
)

data class Tx(
        val hash: String,
        val blockNumber: Long,
        val blockHash: String,
        val coinbase: String? = null,
        val blockTime: Long,
        val size: Int,
        val fee: BigDecimal,
        val totalInputsAmount: BigDecimal,
        val totalOutputsAmount: BigDecimal,
        val ins: List<TxIn>,
        val outs: List<TxOut>
)

data class TxIn(
        val addresses: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val txHash: String,
        val txOut: Int
)

data class TxOut(
        val addresses: List<String>,
        val amount: BigDecimal
)
