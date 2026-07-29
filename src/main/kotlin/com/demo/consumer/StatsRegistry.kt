package com.demo.consumer

import org.springframework.stereotype.Component
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Component
class StatsRegistry {
    val published = AtomicLong(0)

    val unprotectedBusy = AtomicInteger(0)
    val unprotectedSuccess = AtomicLong(0)
    val unprotectedFailed = AtomicLong(0)

    val protectedBusy = AtomicInteger(0)
    val protectedSuccess = AtomicLong(0)
    val protectedDeferred = AtomicLong(0)
    val protectedFailed = AtomicLong(0)

    // How long the unprotected listener pool has been fully saturated.
    // Roughly the point a k8s liveness probe would start timing out.
    @Volatile var unprotectedSaturatedSinceMillis: Long? = null

    // API failure breakdown, mirrors the real karza logs: 503 (fast fail),
    // 504 (slow gateway timeout), 0 (client-side timeout, no response at all).
    val vendorCount503 = AtomicLong(0)
    val vendorCount504 = AtomicLong(0)
    val vendorCountClientTimeout = AtomicLong(0) // "response_code":0 equivalent
    val vendorCallAttempts = AtomicLong(0) // every attempt, including retries
    val vendorRetriesTriggered = AtomicLong(0) // attempts beyond the 1st per user request

    // Small rolling log of circuit breaker state transitions for observability.
    val breakerEvents: MutableList<String> = Collections.synchronizedList(ArrayList())

    fun recordBreakerEvent(text: String) {
        breakerEvents.add(0, "${java.time.LocalTime.now().withNano(0)}  $text")
        while (breakerEvents.size > 20) breakerEvents.removeAt(breakerEvents.size - 1)
    }
}

