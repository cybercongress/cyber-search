package fund.cyber.pump.bitcoin.client.genesis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fund.cyber.common.sum
import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.genesis.GenesisDataProvider
import fund.cyber.search.model.bitcoin.BitcoinBlock
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

        val transactions = genesis.transactions.mapIndexed { index, tx ->
            val ins = tx.ins.map {
                BitcoinTxIn(
                        contracts = it.contracts, amount = it.amount,
                        asm = it.asm, txHash = it.txHash, txOut = it.txOut
                )
            }

            val outs = tx.outs.map {
                BitcoinTxOut(
                        contracts = it.contracts, amount = it.amount, asm = "",
                        out = 0, requiredSignatures = 0
                )
            }

            BitcoinTx(
                    hash = tx.hash, blockNumber =  tx.blockNumber, blockHash = tx.blockHash,
                    coinbase =  tx.coinbase, blockTime = Instant.ofEpochSecond(tx.blockTime),
                    size = tx.size, fee = tx.fee, totalInputsAmount = tx.totalInputsAmount,
                    totalOutputsAmount = tx.totalOutputsAmount, ins = ins, outs = outs, index = index
            )
        }

        val totalOutputsValue = transactions.flatMap { tx -> tx.outs }.map { out -> out.amount }.sum()
        val miner = transactions.first().outs.first().contracts.first()

        val block = BitcoinBlock(
                hash = blockBundle.block.hash, size = blockBundle.block.size,
                minerContractHash =  miner,
                version = blockBundle.block.version, blockReward = blockBundle.block.blockReward,
                txFees = BigDecimal.ZERO, coinbaseData = transactions.first().coinbase?:"",
                bits = blockBundle.block.bits, difficulty = blockBundle.block.difficulty,
                nonce = blockBundle.block.nonce, time = blockBundle.block.time,
                weight = blockBundle.block.weight, merkleroot = blockBundle.block.merkleroot,
                height = blockBundle.block.height,
                txNumber = transactions.size, totalOutputsAmount = totalOutputsValue
        )

        return BitcoinBlockBundle(
                hash = blockBundle.hash, parentHash = blockBundle.parentHash, blockSize = blockBundle.blockSize,
                transactions = transactions, block = block, number = blockBundle.number
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
        val contracts: List<String>,
        val amount: BigDecimal,
        val asm: String,
        val txHash: String,
        val txOut: Int
)

data class TxOut(
        val contracts: List<String>,
        val amount: BigDecimal
)
