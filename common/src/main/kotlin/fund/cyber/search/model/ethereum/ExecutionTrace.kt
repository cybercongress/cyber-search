package fund.cyber.search.model.ethereum

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import fund.cyber.search.model.ethereum.OperationType.CALL
import fund.cyber.search.model.ethereum.OperationType.CREATE_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.DESTROY_CONTRACT
import java.math.BigDecimal

/**
 * Contains full transaction trace tree
 */
data class TxTrace(
    val txHash: String,
    val rootOperationTrace: OperationTrace
)

data class OperationTrace(
    @JsonDeserialize(using = OperationsDeserializer::class)
    val operation: Operation,
    val result: OperationResult,
    val subtraces: List<OperationTrace>
)

sealed class Operation(
    val operationType: OperationType
)

data class CallOperation(
    val type: String,
    val from: String,
    val to: String,
    val input: String,
    val value: BigDecimal,
    val gas: Long
) : Operation(CALL)

data class CreateContractOperation(
    val from: String,
    val init: String,
    val value: BigDecimal,
    val gas: Long
) : Operation(CREATE_CONTRACT)


data class DestroyContractOperation(
    val address: String,
    val balance: BigDecimal,
    val refundAddress: String
) : Operation(DESTROY_CONTRACT)

data class OperationResult(
    val error: String?,
    val address: String?, //todo wtf field?
    val code: String?,//todo wtf field?
    val gasUsed: Long?,
    val output: String?  //raw hex value
)


enum class OperationType {
    CALL, CREATE_CONTRACT, DESTROY_CONTRACT
}


class OperationsDeserializer : JsonDeserializer<Operation>() {

    override fun deserialize(jsonParser: JsonParser, context: DeserializationContext): Operation {
        val objectMapper = jsonParser.codec as ObjectMapper
        val root = objectMapper.readTree<ObjectNode>(jsonParser)
        val operationType = OperationType.valueOf(root["operationType"].textValue())

        return when (operationType) {
            CALL -> objectMapper.convertValue<CallOperation>(root, CallOperation::class.java)
            CREATE_CONTRACT -> objectMapper.convertValue<CreateContractOperation>(root, CreateContractOperation::class.java)
            DESTROY_CONTRACT -> objectMapper.convertValue<DestroyContractOperation>(root, DestroyContractOperation::class.java)
        }
    }

}