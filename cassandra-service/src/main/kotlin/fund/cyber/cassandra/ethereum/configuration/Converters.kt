package fund.cyber.cassandra.ethereum.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.search.model.ethereum.TxTrace
import org.springframework.core.convert.converter.Converter

/**
 * Used to create TxTrace from byted json.
 */
class TxTraceReadConverter(private val jsonDeserializer: ObjectMapper) : Converter<ByteArray, TxTrace> {

    override fun convert(source: ByteArray) = jsonDeserializer.readValue(source, TxTrace::class.java)!!
}

/**
 * Used to convert TxTrace to byted json.
 */
class TxTraceWriteConverter(private val jsonSerializer: ObjectMapper) : Converter<TxTrace, ByteArray> {

    override fun convert(source: TxTrace) = jsonSerializer.writeValueAsBytes(source)!!
}
