package fund.cyber.node.connectors.configuration

import com.fasterxml.jackson.databind.ObjectMapper


object AppContext {
    val jsonSerializer = ObjectMapper()
    val jsonDeserializer = ObjectMapper()
}