package fund.cyber.pump.ethereum.client.genesis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fund.cyber.pump.common.genesis.GenesisDataProvider
import fund.cyber.pump.ethereum.client.EthereumBlockBundle
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.weiToEthRate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Instant

interface EthereumGenesisDataProvider : GenesisDataProvider<EthereumBlockBundle>

@Component
class EthereumGenesisDataFileProvider(
        private val genesisFileRootDirectory: String = "genesis"
) : EthereumGenesisDataProvider {

    override fun provide(blockBundle: EthereumBlockBundle): EthereumBlockBundle {

        val filePath = "/$genesisFileRootDirectory/ethereum.json"

        val jkMapper = ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(KotlinModule())

        val genesis: EthereumGenesisFile = jkMapper
                .readValue(
                        EthereumGenesisDataFileProvider::class.java.getResourceAsStream(filePath),
                        EthereumGenesisFile::class.java
                )

        val txs = genesis.accounts
                .entries
                .filter { (_, value) -> value.balance != null }
                .mapIndexed { index, entry ->

                    val contractHash = entry.key
                    val balance = entry.value.balance

                    val tx = EthereumTx(
                            hash = "GENESIS_$contractHash",
                            nonce = 42,
                            blockHash = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
                            blockNumber = 0,
                            positionInBlock = index,
                            from = "",
                            to = contractHash,
                            value = BigDecimal(balance!!).multiply(weiToEthRate),
                            gasPrice = BigDecimal.ZERO,
                            gasUsed = 0,
                            gasLimit = 0,
                            fee = BigDecimal.ZERO,
                            firstSeenTime = Instant.parse("2015-07-30T15:26:13Z"),
                            blockTime = Instant.parse("2015-07-30T15:26:13Z"),
                            input = "",
                            createdSmartContract = null
                    )

                    listOf(tx)
                }
                .flatten()

        val block = EthereumBlock(
                hash = blockBundle.block.hash, parentHash = blockBundle.block.parentHash,
                number = blockBundle.block.number,
                minerContractHash = blockBundle.block.minerContractHash, difficulty = blockBundle.block.difficulty,
                size = blockBundle.block.size,
                extraData = blockBundle.block.extraData, totalDifficulty = blockBundle.block.totalDifficulty,
                gasLimit = blockBundle.block.gasLimit, gasUsed = blockBundle.block.gasUsed,
                timestamp = Instant.parse("2015-07-30T15:26:13Z"),
                logsBloom = blockBundle.block.logsBloom, transactionsRoot = blockBundle.block.transactionsRoot,
                receiptsRoot = blockBundle.block.receiptsRoot, stateRoot = blockBundle.block.stateRoot,
                sha3Uncles = blockBundle.block.sha3Uncles, uncles = blockBundle.block.uncles,
                txNumber = txs.size, nonce = blockBundle.block.nonce,
                txFees = blockBundle.block.txFees, blockReward = blockBundle.block.blockReward,
                unclesReward = blockBundle.block.blockReward
        )

        return EthereumBlockBundle(
                hash = blockBundle.hash, parentHash = blockBundle.parentHash,
                txes = txs, block = block, number = blockBundle.number,
                uncles = blockBundle.uncles, blockSize = blockBundle.blockSize
        )
    }
}

class EthereumGenesisFile(
        val accounts: Map<String, Balance>
)

class Balance(
        var balance: String?
)
