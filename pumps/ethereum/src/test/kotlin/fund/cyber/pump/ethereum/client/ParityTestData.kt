package fund.cyber.pump.ethereum.client

import fund.cyber.search.jsonDeserializer
import org.web3j.protocol.core.methods.response.EthBlock
import org.web3j.protocol.core.methods.response.EthGetTransactionReceipt
import org.web3j.protocol.core.methods.response.Transaction
import org.web3j.protocol.parity.methods.response.ParityTracesResponse


class ParityTestData {

    fun getBlock(number: Long): EthBlock {

        val blockResourceLocation = javaClass.getResource("/client/parity-data/block/$number.json")
        return jsonDeserializer.readValue(blockResourceLocation, EthBlock::class.java)
    }

    fun getReceipt(txHash: String): EthGetTransactionReceipt {

        val receiptResourceLocation = javaClass.getResource("/client/parity-data/receipts/$txHash.json")
        return jsonDeserializer.readValue(receiptResourceLocation, EthGetTransactionReceipt::class.java)
    }

    fun getTraces(blockNumber: Long): ParityTracesResponse {

        val tracesResourceLocation = javaClass.getResource("/client/parity-data/traces/$blockNumber.json")
        return jsonDeserializer.readValue(tracesResourceLocation, ParityTracesResponse::class.java)
    }

    fun getUncle(blockHash: String, index: Long): EthBlock {

        val uncleResourceLocation = javaClass.getResource("/client/parity-data/uncles/${blockHash}_$index.json")
        return jsonDeserializer.readValue(uncleResourceLocation, EthBlock::class.java)
    }

    fun getTx(hash: String): Transaction {

        val txResourceLocation = javaClass.getResource("/client/parity-data/transaction/$hash.json")
        return jsonDeserializer.readValue(txResourceLocation, Transaction::class.java)
    }

}

