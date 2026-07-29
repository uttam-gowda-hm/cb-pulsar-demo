package com.demo.web

import com.demo.consumer.StatsRegistry
import com.demo.pulsar.MessagePublisher
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class DemoController(
    private val publisher: MessagePublisher,
    private val stats: StatsRegistry,
    private val cbRegistry: CircuitBreakerRegistry,
) {

    @PostMapping("/simulate/start")
    fun start(): Map<String, Any> {
        publisher.start()
        return mapOf("running" to true)
    }

    @PostMapping("/simulate/stop")
    fun stop(): Map<String, Any> {
        publisher.stop()
        return mapOf("running" to false)
    }

    @GetMapping("/stats")
    fun stats(): Map<String, Any> {
        val cb = cbRegistry.circuitBreaker("vendorApi")
        val saturatedSince = stats.unprotectedSaturatedSinceMillis
        val saturatedForSeconds = saturatedSince?.let { (System.currentTimeMillis() - it) / 1000 } ?: 0

        val published = stats.published.get()
        val unprotectedProcessed = stats.unprotectedSuccess.get() + stats.unprotectedFailed.get()
        val protectedProcessed = stats.protectedSuccess.get() + stats.protectedDeferred.get() + stats.protectedFailed.get()

        return mapOf(
            "producerRunning" to publisher.isRunning(),
            "unprotected" to mapOf(
                "poolSize" to 5,
                "busyThreads" to stats.unprotectedBusy.get(),
                "queueBacklog" to (published - unprotectedProcessed).coerceAtLeast(0),
                "success" to stats.unprotectedSuccess.get(),
                "failed" to stats.unprotectedFailed.get(),
                "fullyBlockedForSeconds" to saturatedForSeconds,
                "wouldFailHealthCheck" to (saturatedForSeconds >= 15),
            ),
            "protected" to mapOf(
                "poolSize" to 5,
                "busyThreads" to stats.protectedBusy.get(),
                "queueBacklog" to (published - protectedProcessed).coerceAtLeast(0),
                "success" to stats.protectedSuccess.get(),
                "deferred" to stats.protectedDeferred.get(),
                "failed" to stats.protectedFailed.get(),
                "circuitBreakerState" to cb.state.name,
            ),
            "vendorApi" to mapOf(
                "totalAttempts" to stats.vendorCallAttempts.get(),
                "retriesTriggered" to stats.vendorRetriesTriggered.get(),
                "count503" to stats.vendorCount503.get(),
                "count504" to stats.vendorCount504.get(),
                "countClientTimeout" to stats.vendorCountClientTimeout.get(),
            ),
            "breakerEvents" to stats.breakerEvents.toList(),
        )
    }
}
