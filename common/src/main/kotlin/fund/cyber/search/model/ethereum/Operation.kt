package fund.cyber.search.model.ethereum

import fund.cyber.search.model.ethereum.OperationType.CALL
import fund.cyber.search.model.ethereum.OperationType.CREATE_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.DESTROY_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.REWARD
import java.math.BigDecimal

sealed class Operation(
    val operationType: OperationType
)


/**
 *  List of opcodes: CALL, CALLCODE, DELEGATECALL
 *
 *  Given address[c,d,e] differences between CALL, CALLCODE, DELEGATECALL are:
 *
 *  When D does CALL on E, the code runs in the context of E: the storage of E is used. msg.sender inside E is D
 *  When D does CALLCODE on E, the code runs in the context of D. msg.sender inside E is D
 *  When D does DELEGATECALL on E, the code runs in the context of D. msg.sender inside E is D
 *
 *  When C invokes D, and D does CALLCODE on E, the code runs in the context of D.  msg.sender inside E is D
 *  When C invokes D, and D does DELEGATECALL on E, the code runs in the context of D.  msg.sender inside E is C
 *
 */
data class CallOperation(
    val type: String,
    val from: String,
    val to: String,
    val input: String,
    val value: BigDecimal,
    val gasLimit: Long
) : Operation(CALL)

data class CreateContractOperation(
    val from: String,
    val init: String,
    val value: BigDecimal,
    val gasLimit: Long
) : Operation(CREATE_CONTRACT)


data class DestroyContractOperation(
    val contractToDestroy: String,
    val refundValue: BigDecimal,
    val refundContract: String
) : Operation(DESTROY_CONTRACT)

/**
 * Operations used to provide reward for miners, and uncles.
 */
data class RewardOperation(
    val address: String,
    val value: BigDecimal,
    val rewardType: String
) : Operation(REWARD)


enum class OperationType {
    CALL, CREATE_CONTRACT, DESTROY_CONTRACT, REWARD
}
