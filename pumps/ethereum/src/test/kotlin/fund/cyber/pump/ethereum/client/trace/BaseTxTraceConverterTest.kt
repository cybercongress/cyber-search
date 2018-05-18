package fund.cyber.pump.ethereum.client.trace

import fund.cyber.pump.ethereum.client.toTxesTraces
import fund.cyber.search.jsonDeserializer
import fund.cyber.search.model.ethereum.TxTrace
import org.web3j.protocol.parity.methods.response.Trace

abstract class BaseTxTraceConverterTest {


    protected fun constructTxTrace(txHash: String): TxTrace {
        val flattenTraces = getParityTracesForTx(txHash)
        return toTxesTraces(flattenTraces)[txHash]!!
    }

    protected fun getParityTracesForTx(txHash: String): List<Trace> {

        val tracesResourceLocation = javaClass.getResource("/client/traces/$txHash.json")
        return jsonDeserializer.readValue(tracesResourceLocation, Array<Trace>::class.java).toList()
    }
}
