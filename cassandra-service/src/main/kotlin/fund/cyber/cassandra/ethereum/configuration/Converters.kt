package fund.cyber.cassandra.ethereum.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import fund.cyber.search.model.ethereum.TxTrace
import org.springframework.core.convert.converter.Converter
import java.nio.ByteBuffer

/**
 * Used to create TxTrace from byted json.
 */
class TxTraceReadConverter(private val jsonDeserializer: ObjectMapper) : Converter<ByteBuffer, TxTrace> {

    override fun convert(source: ByteBuffer) = jsonDeserializer.readValue(source.array(), TxTrace::class.java)!!
}

/**
 * Used to convert TxTrace to byted json.
 */
class TxTraceWriteConverter(private val jsonSerializer: ObjectMapper) : Converter<TxTrace, ByteBuffer> {

    override fun convert(source: TxTrace) = ByteBuffer.wrap(jsonSerializer.writeValueAsBytes(source))!!
}
