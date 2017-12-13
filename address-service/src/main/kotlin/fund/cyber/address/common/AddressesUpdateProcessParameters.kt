package fund.cyber.address.common

import fund.cyber.node.common.Chain
import fund.cyber.node.model.CyberSearchItem


data class ConvertEntityToAddressDeltaProcessParameters<T : CyberSearchItem>(
        val inputTopic: String,
        val convertEntityToAddressDeltaFunction: ConvertItemToAddressDeltaFunction<T>,
        val entityType: Class<T>
)


data class AddressesUpdateProcessParameters<D : AddressDelta>(
        val chain: Chain,
        val addressDeltaClassType: Class<D>,
        val convertEntityToAddressDeltaProcessesParameters: List<ConvertEntityToAddressDeltaProcessParameters<*>>,
        val applyAddressDeltaFunction: ApplyAddressDeltaFunction<D>
)