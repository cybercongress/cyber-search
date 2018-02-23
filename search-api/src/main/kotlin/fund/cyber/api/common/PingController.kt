package fund.cyber.api.common

import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController


@RestController
class PingController {

    @GetMapping("/ping")
    @ResponseStatus(HttpStatus.OK)
    fun ping() = {}
}