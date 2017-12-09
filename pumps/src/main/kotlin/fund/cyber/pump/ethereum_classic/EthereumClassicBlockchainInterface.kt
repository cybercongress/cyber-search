package fund.cyber.pump.ethereum_classic

import fund.cyber.cassandra.migration.Migration
import fund.cyber.node.common.Chain.ETHEREUM_CLASSIC
import fund.cyber.node.common.env
import fund.cyber.pump.ethereum.EthereumBlockchainInterface


class EthereumClassicBlockchainInterface : EthereumBlockchainInterface(
        parityUrl = env("PARITY_ETC", "http://127.0.0.1:18545"),
        network = ETHEREUM_CLASSIC
) {

    override val migrations: List<Migration> = EthereumClassicMigrations.migrations
}