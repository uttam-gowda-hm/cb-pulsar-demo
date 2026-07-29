package com.demo.vendor

import com.demo.consumer.StatsRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.kotlin.circuitbreaker.executeSuspendFunction
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeout
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.awaitBody

@Component
class VendorClient(
    webClientBuilder: WebClient.Builder,
    circuitBreakerRegistry: CircuitBreakerRegistry,
    private val stats: StatsRegistry,
) {
    private val webClient = webClientBuilder.baseUrl("http://localhost:8082").build()
    private val circuitBreaker = circuitBreakerRegistry.circuitBreaker("vendorApi")

    private val concurrencyLimiter = Semaphore(permits = 5)

    @PostConstruct
    fun registerBreakerObservability() {
        circuitBreaker.eventPublisher.onStateTransition {
            stats.recordBreakerEvent("breaker vendorApi: ${it.stateTransition.fromState} -> ${it.stateTransition.toState}")
        }
        circuitBreaker.eventPublisher.onCallNotPermitted {
            stats.recordBreakerEvent("call rejected - breaker OPEN, no retries attempted")
        }
    }

    private fun classify(e: Throwable) {
        when {
            e is WebClientResponseException && e.statusCode.value() == 504 -> stats.vendorCount504.incrementAndGet()
            e is WebClientResponseException && e.statusCode.value() == 503 -> stats.vendorCount503.incrementAndGet()
            else -> stats.vendorCountClientTimeout.incrementAndGet() // handshake/response timeout, no status code - the "0" bucket
        }
    }

    /** One call, retried up to 3x - mirrors KarzaService's real retry-per-user behavior. */
    private suspend fun callWithRetries(perAttemptTimeoutMs: Long?): String {
        var lastError: Throwable? = null
        repeat(3) { attempt ->
            stats.vendorCallAttempts.incrementAndGet()
            if (attempt > 0) stats.vendorRetriesTriggered.incrementAndGet()
            try {
                return if (perAttemptTimeoutMs != null) {
                    withTimeout(perAttemptTimeoutMs) {
                        webClient.get().uri("/vendor/verify").retrieve().awaitBody()
                    }
                } else {
                    webClient.get().uri("/vendor/verify").retrieve().awaitBody()
                }
            } catch (e: Exception) {
                classify(e)
                lastError = e // real service logs "retryCount: N" here before trying again
            }
        }
        throw lastError ?: IllegalStateException("vendor call failed after retries")
    }

    /** Baseline: same 3x retry, no timeout, no breaker - exactly what amplified the real incident. */
    suspend fun callDirect(): String = callWithRetries(perAttemptTimeoutMs = null)

    /**
     * Protected path: while CLOSED, still retries up to 3x (bounded per-attempt
     * by the timeout) same as production. Once OPEN, executeSuspendFunction
     * throws immediately and the retry loop never runs at all - this is the
     * real payoff: a struggling vendor stops receiving 3x the load per failed
     * user request the moment the breaker trips.
     */
    suspend fun callProtected(): String = try {
        circuitBreaker.executeSuspendFunction {
            concurrencyLimiter.withPermit {
                callWithRetries(perAttemptTimeoutMs = 1200)
            }
        }
    } catch (e: CallNotPermittedException) {
        "DEFERRED" // breaker OPEN - zero retries attempted, zero extra load on vendor
    } catch (e: Exception) {
        "DEFERRED"
    }
}

