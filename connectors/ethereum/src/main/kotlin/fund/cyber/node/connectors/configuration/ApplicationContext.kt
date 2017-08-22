package fund.cyber.node.connectors.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient


object ApplicationContext {
    val httpClient = OkHttpClient()
    val jsonParser = ObjectMapper()
}