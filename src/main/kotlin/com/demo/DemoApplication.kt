package com.demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * Sample incident, same shape as a real one: a partner/vendor verification
 * call starts timing out, and it's called from inside a Pulsar message
 * listener. Two consumer pools run side by side against a real local Pulsar
 * broker (via docker-compose) - one unprotected (blocking call, no timeout),
 * one wrapped with a circuit breaker + coroutine timeout + bulkhead.
 */
@SpringBootApplication
@EnableScheduling
class DemoApplication

fun main(args: Array<String>) {
    runApplication<DemoApplication>(*args)
}
