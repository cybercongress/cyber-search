package fund.cyber.address.bitcoin

import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcessParameters
import fund.cyber.address.common.addressCache
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.BitcoinAddress
import fund.cyber.node.model.BitcoinTransaction
import org.ehcache.CacheManager


fun getBitcoinAddressesUpdateProcessParameters(
        chain: Chain, keyspaceRepository: BitcoinKeyspaceRepository, cacheManager: CacheManager
): AddressesUpdateProcessParameters {

    val transactionToAddressDeltaProcessesParameters = ConvertEntityToAddressDeltaProcessParameters(
            entityType = BitcoinTransaction::class.java,
            convertEntityToAddressDeltaFunction = BitcoinTransactionToAddressDeltaFunction(),
            inputTopic = chain.entityTopic(TRANSACTION)
    )

    val addressCache = cacheManager.getCache(chain.addressCache, String::class.java, BitcoinAddress::class.java)
    val applyAddressDeltaFunction = ApplyBitcoinAddressDeltaFunction(keyspaceRepository, addressCache)

    return AddressesUpdateProcessParameters(
            chain = chain, applyAddressDeltaFunction = applyAddressDeltaFunction,
            convertEntityToAddressDeltaProcessesParameters = listOf(transactionToAddressDeltaProcessesParameters)
    )
}