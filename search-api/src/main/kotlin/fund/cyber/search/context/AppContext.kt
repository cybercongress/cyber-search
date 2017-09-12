package fund.cyber.search.context

import com.fasterxml.jackson.databind.ObjectMapper

object AppContext {
    val jsonSerializer = ObjectMapper()
    val jsonDeserializer = ObjectMapper()
}