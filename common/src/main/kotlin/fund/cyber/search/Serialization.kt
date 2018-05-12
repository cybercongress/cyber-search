package fund.cyber.search

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

val jsonSerializer = ObjectMapper().registerKotlinModule()
        .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)!!

val jsonDeserializer = ObjectMapper().registerKotlinModule()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)!!
