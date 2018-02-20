package fund.cyber.pump.ethereum.client.genesis

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fund.cyber.pump.common.genesis.GenesisDataProvider
import fund.cyber.pump.ethereum.client.EthereumBlockBundle
import fund.cyber.search.model.chains.Chain
import fund.cyber.search.model.ethereum.EthereumTransaction
import fund.cyber.search.model.ethereum.weiToEthRate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Instant

interface EthereumGenesisDataProvider : GenesisDataProvider<EthereumBlockBundle>

@Component
class EthereumGenesisDataFileProvider(
        private val genesisFileRootDirectory: String = "genesis",
        private val chain: Chain
) : EthereumGenesisDataProvider {

    override fun provide(blockBundle: EthereumBlockBundle): EthereumBlockBundle {

        val filePath = "/$genesisFileRootDirectory/${chain.lowerCaseName}.json"

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
                .mapIndexed { _, entry ->

                    val addressId = entry.key
                    val balance = entry.value.balance

                    val tx = EthereumTransaction(
                            hash = "GENESIS_$addressId",
                            nonce = 42,
                            block_hash = "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
                            block_number = 0,
                            transaction_index = 0,
                            from = "",
                            to = addressId,
                            value = BigDecimal(balance!!).multiply(weiToEthRate),
                            gas_price = BigDecimal.ZERO,
                            gas_used = 0,
                            gas_limit = 0,
                            fee = BigDecimal.ZERO,
                            block_time = Instant.parse("2015-07-30T15:26:13Z"),
                            input = "",
                            creates = ""
                    )

                    listOf(tx)
                }
                .flatten()

        return EthereumBlockBundle(
                hash = blockBundle.hash, parentHash = blockBundle.parentHash,
                transactions = txs, block = blockBundle.block, number = blockBundle.number,
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