package fund.cyber.node.connectors.configuration

import fund.cyber.node.helpers.env


class ApplicationConfiguration(
        val parity_url: String = env("PARITY_URL", "http://127.0.0.1:8545")
)