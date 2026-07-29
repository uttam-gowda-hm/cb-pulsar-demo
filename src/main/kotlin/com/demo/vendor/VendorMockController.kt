package com.demo.vendor

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Stands in for a lending/verification partner API. Toggle its behaviour to
 * reproduce the incident: partner is technically "up" but responding slowly
 * enough to be indistinguishable from down, from the caller's point of view.
 */
@RestController
@RequestMapping("/vendor")
class VendorMockController {

    private val delayMillis = AtomicLong(100)
    private val healthy = AtomicBoolean(true)
    private val failStatus = AtomicLong(503)
    private val karzaOutage = AtomicBoolean(false)

    @PostMapping("/toggle")
    fun toggle(
        @RequestParam(defaultValue = "100") delayMillis: Long,
        @RequestParam(defaultValue = "true") healthy: Boolean,
        @RequestParam(defaultValue = "503") failStatus: Long,
    ): Map<String, Any> {
        karzaOutage.set(false)
        this.delayMillis.set(delayMillis)
        this.healthy.set(healthy)
        this.failStatus.set(failStatus)
        return mapOf("delayMillis" to delayMillis, "healthy" to healthy, "failStatus" to failStatus)
    }

    /** Reproduces the api.karza.in/v3/name pattern: mostly fast 503s, some slow
     *  504s (~29s), and some so slow the client's own timeout fires first
     *  (client-side timeout / connection reset, "response_code 0" in real logs). */
    @PostMapping("/scenario/karza-outage")
    fun karzaOutageScenario(@RequestParam(defaultValue = "true") on: Boolean): Map<String, Any> {
        karzaOutage.set(on)
        return mapOf("karzaOutage" to on)
    }

    @GetMapping("/verify")
    fun verify(): Mono<ResponseEntity<Map<String, String>>> {
        if (karzaOutage.get()) {
            val roll = Math.random()
            val (delay, status) = when {
                roll < 0.6 -> 20L to 503        // fast fail, most common
                roll < 0.85 -> 4500L to 504     // slow gateway timeout
                else -> 12000L to 0             // exceeds client timeout -> client-side timeout, no response
            }
            return Mono.delay(Duration.ofMillis(delay)).map {
                ResponseEntity.status(if (status == 0) 503 else status).body(mapOf("status" to "DOWN"))
            }
        }
        return Mono.delay(Duration.ofMillis(delayMillis.get())).map {
            if (healthy.get()) {
                ResponseEntity.ok(mapOf("status" to "VERIFIED"))
            } else {
                ResponseEntity.status(HttpStatus.valueOf(failStatus.get().toInt())).body(mapOf("status" to "DOWN"))
            }
        }
    }
}
