package fund.cyber.pump.ethereum

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import fund.cyber.cassandra.migration.CassandraEntityMigration
import fund.cyber.node.common.Chain
import fund.cyber.node.model.*
import java.math.BigDecimal
import java.time.Instant


class GenesisMigration(
        override val version: Int,
        override val applicationId: String,
        override val chain: Chain,
        private val filePath: String
) : CassandraEntityMigration {

    override val entities: List<CyberSearchItem>
        get() {
            val jkMapper = ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).registerModule(KotlinModule())
            val genesis: GenesisFile = jkMapper.readValue(GenesisMigration::class.java.getResourceAsStream(filePath), GenesisFile::class.java)
            return genesis.accounts
                    .entries
                    .filter { (_, value) -> value.balance != null }
                    .mapIndexed { index, entry ->

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
                                value = balance!!,
                                gas_price = BigDecimal.ZERO,
                                gas_used = 0,
                                gas_limit = 0,
                                fee = "0",
                                block_time = Instant.parse("2015-07-30T15:26:13Z"),
                                input = "",
                                creates = ""
                        )
                        val blockTx = EthereumBlockTxPreview(tx, index)
                        val addressTx = EthereumAddressTxPreview(
                                address = addressId,
                                fee = tx.fee,
                                block_time = tx.block_time,
                                hash = tx.hash,
                                from = tx.from,
                                to = tx.to!!,
                                value = tx.value
                        )
                        val address = EthereumAddress(
                                id = addressId,
                                balance = tx.value,
                                contract_address = false,
                                total_received = tx.value,
                                last_transaction_block = 0,
                                tx_number = 0,
                                mined_block_number = 0,
                                uncle_number = 0
                        )
                        listOf(tx, blockTx, addressTx, address)
                    }
                    .flatten()
        }
}


class GenesisFile(
        var accounts: Map<String, Balance>
)

class Balance(
        var balance: String?
)
