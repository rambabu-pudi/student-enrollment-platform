# Student Enrollment Platform

Event-driven microservice platform that ingests enrollment facts from **CSV**, **XML** and
**JSON/REST (Kafka-driven)** sources, deduplicates them globally, and serves a live dashboard.

**Target scale:** 1,000,000 records/hour sustained, 24,000,000 records total.
**Stack:** Java 21 · Spring Boot 3.3 · Apache Kafka · PostgreSQL 16 · Gradle · Docker.

---

## 1. The problem, restated

Three heterogeneous input channels deliver the same underlying fact:

> *Student `S` is enrolled in class `C` at university `U`.*

The dashboard must render the latest state as:

```
S1, U1, [C1, C2, C3]
S1, U2, [C3]
S2, U2, [C1, C3]
```

The hard requirement is that `S1,U1,C1` must appear **exactly once**, whether it arrives
twice in one CSV, once in a CSV and once in a JSON batch, or is replayed after a Kafka
rebalance.

---

## 2. High-level architecture

```
  CSV file ─┐
  XML file ─┤   multipart upload
            ├──►  ┌──────────────────────┐
  JSON/REST ┘     │  ingestion-service   │  Strategy + Factory parsers
  (Kafka-driven   │  :8081               │  streaming, constant memory
   upstream)      └──────────┬───────────┘  L1 dedup (in-request)
                             │
                             │  Kafka  enrollment.ingested.v1
                             │  key = studentId, 24 partitions, lz4
                             │  acks=all, idempotent producer
                             ▼
                  ┌──────────────────────┐
                  │ processing-service   │  batch consumer (2000/poll)
                  │ :8082    xN pods     │  L2 in-batch dedup
                  └──────────┬───────────┘  L3 DB dedup (authoritative)
                             │                     │
                             │                     └──► enrollment.ingested.v1.DLT
                             ▼
                  ┌──────────────────────┐
                  │     PostgreSQL       │
                  │  enrollment          │  write model, HASH-partitioned x8
                  │  dashboard_student_  │  read model (CQRS projection)
                  │     university       │
                  └──────────┬───────────┘
                             │  read replica
                             ▼
                  ┌──────────────────────┐
                  │    query-service     │  keyset-paginated dashboard API
                  │    :8083             │  never scans the 24M-row table
                  └──────────────────────┘
```

### Why this shape

| Decision | Rationale |
|---|---|
| **Ingestion decoupled from persistence via Kafka** | The write path can accept bursts far above 1M/hour; Kafka is the shock absorber. A database outage degrades to lag, not to rejected uploads. |
| **Separate read service + projection table** | The dashboard must never run `GROUP BY` over 24M rows. Read and write scale independently. |
| **Partition Kafka by `studentId`** | Guarantees per-student ordering and means one student is only ever processed by one consumer — removing cross-pod contention on the same projection row. |
| **Natural key as the primary key** | Deduplication becomes a database invariant, not application logic that a second pod can race past. |

---

## 3. Deduplication — the core requirement

Deduplication is **layered**, and each layer exists for a reason:

| Layer | Where | Mechanism | Purpose |
|---|---|---|---|
| **L1** | `ingestion-service` | `BoundedDeduplicationFilter` over deterministic event ids | Collapses repeats inside one file/request so they never reach the wire. Bounded window — a 24M-row file cannot exhaust heap. |
| **L2** | `processing-service` | `HashSet` of event ids per Kafka poll | Ensures the JDBC batch carries no self-conflicts. |
| **L3** | **PostgreSQL** | `PRIMARY KEY (student_id, university_id, class_code)` + `ON CONFLICT DO NOTHING` | **Authoritative.** Race-free across pods, files, sources and Kafka redeliveries. |

The linchpin is the **deterministic event id** (`DeduplicationKeys`): the triple
`(S1, U1, C1)` always hashes to the same UUID — in any service, from any input format, on any
JVM. Values are normalised (trim + upper-case) at construction, so `" s1 "`, `"S1"` and `"s1"`
are the same fact.

L1 and L2 are optimisations; **L3 is the guarantee.** Losing a duplicate in the bounded L1
window is harmless because L3 still catches it.

The dashboard projection merge is itself idempotent (`array_agg(DISTINCT …)`), so
at-least-once Kafka delivery cannot corrupt the read model.

---

## 4. Meeting 1M records/hour (and bursting well past it)

1M/hour ≈ **278 records/second** sustained. The design targets an order of magnitude more
headroom so the 24M backfill completes in hours, not days.

| Lever | Setting | Effect |
|---|---|---|
| Streaming parsers | StAX / Jackson streaming / OpenCSV row-by-row | Flat memory regardless of file size — a 2 GB CSV never lands on the heap. |
| Producer batching | `linger.ms=20`, `batch.size=64KB`, `compression=lz4` | Trades 20 ms latency for a large throughput gain; 3–5× smaller payloads on the wire. |
| Idempotent producer | `acks=all` + `enable.idempotence=true` | No-loss, no-duplicate publishing without a transaction coordinator. |
| **Batch consumption** | `max.poll.records=2000`, batch listener | **One DB round trip per poll instead of per record — the single biggest lever.** |
| Consumer concurrency | 8 threads/pod, 24 partitions | Scales linearly to 24 concurrent consumers across pods. |
| JDBC batch upsert | `reWriteBatchedInserts=true` | Postgres collapses the batch into multi-row `INSERT`s: 3–5× faster. |
| Plain JDBC, not JPA | `JdbcTemplate` | Avoids persistence-context overhead that dominates cost in append-only upserts. |
| Table partitioning | `HASH (student_id)` × 8 | Keeps each partition's index resident in `shared_buffers` at 24M rows, so upsert latency stays flat. |
| Projection write suppression | `WHERE NOT (class_codes @> excluded.class_codes)` | No-op redeliveries create no dead tuples, avoiding autovacuum churn. |
| Keyset pagination | cursor, not `OFFSET` | Page 10,000 costs the same as page 1. |

**Scaling out:** add `processing-service` pods up to 24 (the partition count). Throughput is
linear in pod count until then; beyond that, raise the partition count.

---

## 5. Availability and failure handling

| Concern | Mitigation |
|---|---|
| Bad/poison message | `ErrorHandlingDeserializer` → null payload instead of an infinite container crash loop. |
| Transient DB failure | `DefaultErrorHandler` with exponential backoff (500 ms → 60 s cap). |
| Permanently failing record | `DeadLetterPublishingRecoverer` → `enrollment.ingested.v1.DLT`; the partition keeps moving. |
| Consumer crash mid-batch | Manual `MANUAL_IMMEDIATE` ack **after** commit. Offsets never advance past unpersisted data; redelivery is safe because writes are idempotent. |
| Deadlocks between pods | Events sorted by natural key → global lock order; plus student-keyed partitioning. |
| Rolling deploys | Graceful shutdown, liveness/readiness probes, `max.poll.interval.ms=300000`. |
| Broker/DB loss | `acks=all` + replication factor 3 in production; Postgres streaming replication + read replica for `query-service`. |

---

## 6. Design patterns applied

| Pattern | Where | Why it earns its place |
|---|---|---|
| **Strategy** | `EnrollmentPayloadParser` + CSV/XML/JSON implementations | Adding a format (Avro, fixed-width) means adding one class. |
| **Factory** | `EnrollmentParserFactory` | Content negotiation isolated from the controller. |
| **Canonical Data Model** | `EnrollmentRecord` | All three channels share one dedup + persistence pipeline — which is *why* cross-format dedup works. |
| **Factory Method** | `EnrollmentEventFactory` | No caller can create a non-deterministic event id. |
| **CQRS** | `processing-service` writes / `query-service` reads | Read and write models optimised and scaled independently. |
| **Idempotent Receiver** | Deterministic ids + `ON CONFLICT DO NOTHING` | Makes at-least-once delivery safe. |
| **Dead Letter Channel** | `…v1.DLT` | One bad record cannot stall a partition. |
| **Retry with Backoff / Circuit protection** | `DefaultErrorHandler`, Resilience4j | Protects downstream dependencies. |
| **Repository** | `EnrollmentWriteRepository`, `DashboardQueryRepository` | Persistence detail isolated from domain logic. |
| **Externalised Configuration** | `IngestionProperties`, env-driven YAML | Same artifact across environments (12-factor). |
| **Database per Service** | `processing-service` owns Flyway; `query-service` is read-only | Single writer owns the schema. |
| **Health Check API / Observability** | Actuator + Micrometer/Prometheus | Probes and per-stage throughput metrics. |
| **Constructor Injection** | Everywhere | Immutable, testable components. |

---

## 7. Data model

**Write model** — `enrollment`, `HASH (student_id)` partitioned into 8:

```sql
PRIMARY KEY (student_id, university_id, class_code)   -- the deduplication guarantee
```

**Read model** — `dashboard_student_university`, one row per `(student, university)`:

| student_id | university_id | class_codes | class_count | updated_at |
|---|---|---|---|---|
| S1 | U1 | `{C1,C2,C3}` | 3 | … |
| S1 | U2 | `{C3}` | 1 | … |
| S2 | U2 | `{C1,C3}` | 2 | … |

That is the required dashboard shape, materialised — no aggregation at query time.

---

## 8. Running it

### Everything in Docker

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Ingestion API | http://localhost:8081/swagger-ui.html |
| Dashboard API | http://localhost:8083/swagger-ui.html |
| Postgres | `localhost:5432` (`enrollment` / `enrollment`) |
| Kafka | `localhost:9092` |

Scale the consumer tier:

```bash
docker compose up -d --scale processing-service=4
```

### Local build and test

```bash
./gradlew build          # compiles all modules and runs the test suite
./gradlew :ingestion-service:bootRun
```

---

## 9. End-to-end walkthrough

```bash
# 1. CSV — contains deliberate duplicates
curl -F "file=@samples/enrollments.csv" http://localhost:8081/api/v1/enrollments/files

# 2. XML — re-sends S1,U1,C1 which already exists
curl -F "file=@samples/enrollments.xml" http://localhost:8081/api/v1/enrollments/files

# 3. JSON batch — the endpoint a Kafka-driven upstream calls
curl -X POST http://localhost:8081/api/v1/enrollments \
     -H "Content-Type: application/json" \
     -d @samples/enrollments.json

# 4. Dashboard — deduplicated, latest first
curl "http://localhost:8083/api/v1/dashboard?limit=10"

# 5. Drill down on one student
curl http://localhost:8083/api/v1/dashboard/students/S1
```

Across all three files, `S1,U1,C1` is submitted **four times**. The dashboard returns it once:

```json
{
  "rows": [
    { "studentId": "S1", "universityId": "U1", "classes": ["C1","C2","C3"], "classCount": 3 },
    { "studentId": "S1", "universityId": "U2", "classes": ["C3"],           "classCount": 1 },
    { "studentId": "S2", "universityId": "U2", "classes": ["C1","C3"],      "classCount": 2 },
    { "studentId": "S3", "universityId": "U1", "classes": ["C7"],           "classCount": 1 }
  ],
  "nextCursor": null,
  "hasMore": false
}
```

---

## 10. API reference

### ingestion-service (`:8081`)

| Method | Path | Body | Purpose |
|---|---|---|---|
| `POST` | `/api/v1/enrollments` | JSON batch | REST/Kafka channel |
| `POST` | `/api/v1/enrollments/files` | multipart `file` | CSV / XML / JSON upload |

Both return `202 Accepted`:

```json
{ "correlationId":"…", "recordsParsed":8, "recordsAccepted":6,
  "duplicatesSuppressed":2, "durationMillis":14, "status":"ACCEPTED" }
```

### query-service (`:8083`)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/dashboard?cursor=&limit=` | Latest rows, keyset paginated |
| `GET` | `/api/v1/dashboard/students/{studentId}` | All universities/classes for a student |
| `GET` | `/api/v1/dashboard/universities/{universityId}` | All students at a university |
| `GET` | `/api/v1/dashboard/students/{s}/universities/{u}` | One cell |

---

## 11. Module layout

```
student-enrollment-platform/
├── common/              canonical model, event contract, deterministic dedup keys
├── ingestion-service/   parsers (Strategy/Factory), L1 dedup, Kafka producer
├── processing-service/  batch consumer, L2+L3 dedup, Flyway schema, CQRS projection
├── query-service/       read-only dashboard API
├── samples/             CSV / XML / JSON payloads with intentional duplicates
├── k8s/                 Kubernetes deployment + HPA example
├── Dockerfile           multi-stage, parameterised by MODULE
└── docker-compose.yml   Kafka (KRaft) + Postgres + all three services
```

---

## 12. Production checklist

Deliberately **not** included here, and required before go-live:

- [ ] Authentication/authorisation on both APIs (OAuth2 resource server / mTLS).
- [ ] Kafka `replication.factor=3`, `min.insync.replicas=2`, TLS + SASL.
- [ ] Postgres HA (streaming replication, automated failover); point `query-service` at the replica.
- [ ] Secrets from a vault, not environment variables.
- [ ] Distributed tracing (OpenTelemetry) — the `correlationId` header is already propagated.
- [ ] Alerting on consumer lag, DLT depth, and `enrollment.processing.duplicates` rate.
- [ ] Load test at 3× target to validate headroom before the 24M backfill.
