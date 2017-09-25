package fund.cyber.search.configuration

import fund.cyber.node.common.env


class SearchApiConfiguration(
        val elasticHost: String = env("ELASTIC_HOST_CONNECTION", "localhost"),
        val elasticPort: Int = env("ELASTIC_PORT_CONNECTION", 9300),
        val elasticClusterName: String = env("ELASTIC_CLUSTER_NAME", "CYBERNODE")
)
