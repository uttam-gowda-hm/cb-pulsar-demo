# Start the local Pulsar broker
docker compose up -d

# Run the Spring Boot application (runs on http://localhost:8082)
gradle bootRun
```[cite: 3]

---

### 2. Run the Incident Simulation
1. **Open the Dashboard**: Navigate to `http://localhost:8082` in your browser[cite: 3].
2. **Start Traffic**:
   ```bash
   curl -X POST localhost:8082/simulate/start
   ```[cite: 3]
3. **Trigger Timeouts / Retries (Simulate Failure)**:
   ```bash
   curl -X POST "localhost:8082/vendor/toggle?delayMillis=8000&healthy=true"
   ```[cite: 3]
4. **Observe the Difference**:
   * **Unprotected Consumer**: All 5 listener threads immediately block while waiting on retries/timeouts[cite: 3]. The message backlog grows, and after ~15s, Kubernetes liveness health checks fail[cite: 3].
   * **Protected Consumer**: After ~5 slow/failed calls, the circuit breaker trips from `CLOSED` to `OPEN`[cite: 3]. All subsequent requests fail fast immediately without waiting on timeouts or wasting retries[cite: 2, 3].
5. **Recover the Vendor**:
   ```bash
   curl -X POST "localhost:8082/vendor/toggle?delayMillis=100&healthy=true"
   ```[cite: 3]
6. **Watch Self-Healing**:
   The circuit breaker transitions `OPEN → HALF_OPEN → CLOSED` after 8 seconds[cite: 3]. The deferred messages are redelivered by Pulsar and processed successfully[cite: 3].
7. **Stop Traffic**:
   ```bash
   curl -X POST localhost:8082/simulate/stop
   ```[cite: 3]

---

## How the Circuit Breaker Helps Here