package fund.cyber.cassandra.ethereum.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.search.model.ethereum.TxTrace
import org.springframework.core.convert.converter.Converter

/**
 * Used to create TxTrace from stringified json.
 */
class TxTraceReadConverter(private val jsonDeserializer: ObjectMapper) : Converter<String, TxTrace> {

    override fun convert(source: String) = jsonDeserializer.readValue(source, TxTrace::class.java)!!
}

/**
 * Used to convert TxTrace to stringified json.
 */
class TxTraceWriteConverter(private val jsonSerializer: ObjectMapper) : Converter<TxTrace, String> {

    override fun convert(source: TxTrace) = jsonSerializer.writeValueAsString(source)!!
}
