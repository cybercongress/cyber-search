package fund.cyber.node.kafka

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets.UTF_8

class JsonSerializer<T> : Serializer<T> {
    private val objectMapper = ObjectMapper().registerKotlinModule()

    override fun serialize(topic: String, data: T): ByteArray {
        return objectMapper.writeValueAsString(data).toByteArray()
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
}

class JsonDeserializer<T>(private val type: Class<T>) : Deserializer<T> {
    private val objectMapper = ObjectMapper().registerKotlinModule()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    override fun deserialize(topic: String, data: ByteArray): T {
        return objectMapper.readValue(data, type)
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {}
    override fun close() {}
}
