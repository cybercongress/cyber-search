package fund.cyber.index.ethereum.converter

import fund.cyber.node.model.*
import org.slf4j.LoggerFactory
import java.math.BigDecimal

private val log = LoggerFactory.getLogger(EthereumAddressConverter::class.java)!!

class EthereumAddressConverter {


    fun updateAddressesSummary(
            newBlock: EthereumBlock, transactions: List<EthereumTransaction>,
            existingAddressesUsedInBlock: List<EthereumAddress>): List<EthereumAddress> {

        val addressLookUp = existingAddressesUsedInBlock
                .associateBy { address -> address.id }.toMutableMap()

        val updatedAddresses = transactions
                .flatMap { tx ->
                    listOf(
                            updateAddressByTransaction(newBlock.number, addressLookUp[tx.from]!!, tx),
                            updateAddressByInputTransaction(newBlock.number, addressLookUp[tx.to], tx.creates ?: tx.to!!, tx)
                    ).apply { this.forEach { address -> addressLookUp.put(address.id, address) } }
                }

        val updatedMiner = updateAdressesByMiner(newBlock, addressLookUp[newBlock.miner])
        addressLookUp.put(updatedMiner.id, updatedMiner)

        return updatedAddresses.plus(updatedMiner)
    }

    fun updateAdressesByMiner(newBlock: EthereumBlock, address: EthereumAddress?): EthereumAddress {

        val blockReward = getBlockReward(newBlock.number)
        val uncleReward = BigDecimal(newBlock.uncles.size) / BigDecimal("32")
        val txReward = BigDecimal(newBlock.tx_fees)
        val finalReward = blockReward + uncleReward + txReward

        val txNumber = address?.tx_number ?: 0
        val balance = BigDecimal(address?.balance ?: "0") + finalReward
        val totalReceived = BigDecimal(address?.total_received ?: "0") + finalReward

        return EthereumAddress(
                id = newBlock.miner, last_transaction_block = newBlock.number, tx_number = txNumber,
                balance = balance.toString(), total_received = totalReceived.toString(),
                contract_address = address?.contract_address ?: false
        )
    }


    fun updateAddressByInputTransaction(blockNumber: Long, address: EthereumAddress?,
                                        addressId: String, tx: EthereumTransaction): EthereumAddress {

        if (address == null) {
            log.trace("first address transaction: $addressId")
            return EthereumAddress(
                    id = addressId, last_transaction_block = blockNumber, tx_number = 1,
                    balance = tx.value, total_received = tx.value, contract_address = tx.creates != null
            )
        }
        return updateAddressByTransaction(blockNumber, address, tx)
    }


    fun updateAddressByTransaction(
            blockNumber: Long, address: EthereumAddress, tx: EthereumTransaction): EthereumAddress {

        val isOutgoingTransaction = tx.from == address.id

        val balance =
                if (isOutgoingTransaction) BigDecimal(address.balance) - BigDecimal(tx.value)
                else BigDecimal(address.balance) + BigDecimal(tx.value)

        val totalReceived =
                if (isOutgoingTransaction) address.total_received
                else (BigDecimal(address.total_received) + BigDecimal(tx.value)).toString()

        return EthereumAddress(
                id = address.id, last_transaction_block = blockNumber, tx_number = address.tx_number + 1,
                balance = balance.toString(), total_received = totalReceived, contract_address = address.contract_address
        )
    }


    private fun nonExistingAddressFromOutput(
            blockNumber: Long, addressId: String, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = addressId, last_transaction_block = blockNumber, tx_number = 1,
                total_received = output.amount, balance = output.amount
        )
    }


    private fun updateAddressByTransactionOutput(
            blockNumber: Long, address: BitcoinAddress, output: BitcoinTransactionOut): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, last_transaction_block = blockNumber,
                tx_number = address.tx_number + 1,
                total_received = (BigDecimal(address.total_received) + BigDecimal(output.amount)).toString(),
                balance = (BigDecimal(address.balance) + BigDecimal(output.amount)).toString()
        )
    }


    private fun updateAddressByTransactionInput(
            blockNumber: Long, address: BitcoinAddress, input: BitcoinTransactionIn): BitcoinAddress {

        return BitcoinAddress(
                id = address.id, last_transaction_block = blockNumber,
                tx_number = address.tx_number + 1, total_received = address.total_received,
                balance = (BigDecimal(address.balance) - BigDecimal(input.amount)).toString()
        )
    }


    fun transactionsPreviewsForAddresses(newBlock: EthereumBlock, transactions: List<EthereumTransaction>): List<EthereumAddressTransaction> {

        return transactions
                .flatMap { tx ->
                    tx.addressesUsedInTransaction().map { addressId ->
                        EthereumAddressTransaction(
                                address = addressId, fee = tx.fee, block_time = newBlock.timestamp,
                                hash = tx.hash, value = tx.value, from = tx.from, to = tx.creates ?: tx.to!!
                        )
                    }
                }
    }
}