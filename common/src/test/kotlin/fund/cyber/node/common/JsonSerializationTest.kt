package fund.cyber.node.common

import fund.cyber.search.jsonDeserializer
import fund.cyber.search.jsonSerializer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.math.BigDecimal

@DisplayName("Json serialization/deserialization test")
class JsonSerializationTest {

    @Test
    @DisplayName("should BigDecimal serialized to Json string")
    fun testBigDecimalShouldBeSerializedAsJsonString() {

        val valueAsString = "0.32784291897287934505879230273459823424"
        val classWithMoney = ClassWithMoney(BigDecimal(valueAsString))
        val classAsString = jsonSerializer.writeValueAsString(classWithMoney)
        val deserializedClass = jsonDeserializer.readValue(classAsString, ClassWithMoney::class.java)

        Assertions.assertEquals("""{"money":"0.32784291897287934505879230273459823424"}""", classAsString)
        Assertions.assertEquals(classWithMoney, deserializedClass)
    }
}


private data class ClassWithMoney(
    val money: BigDecimal
)
