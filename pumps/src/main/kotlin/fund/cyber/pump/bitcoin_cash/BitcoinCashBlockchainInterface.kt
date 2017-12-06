package fund.cyber.pump.bitcoin_cash

import fund.cyber.dao.migration.Migration
import fund.cyber.node.common.Chain
import fund.cyber.pump.bitcoin.BitcoinBlockchainInterface


class BitcoinCashBlockchainInterface : BitcoinBlockchainInterface(
        rpcToBundleEntitiesConverter = BitcoinCashPumpContext.rpcToBundleEntitiesConverter,
        bitcoinJsonRpcClient = BitcoinCashPumpContext.bitcoinJsonRpcClient
) {
    override val chain = Chain.BITCOIN_CASH
    override val migrations: List<Migration> = BitcoinCashMigrations.migrations
}