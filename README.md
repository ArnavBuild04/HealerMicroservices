<div align="center">

```
██╗  ██╗███████╗ █████╗ ██╗     ███████╗██████╗
██║  ██║██╔════╝██╔══██╗██║     ██╔════╝██╔══██╗
███████║█████╗  ███████║██║     █████╗  ██████╔╝
██╔══██║██╔══╝  ██╔══██║██║     ██╔══╝  ██╔══██╗
██║  ██║███████╗██║  ██║███████╗███████╗██║  ██║
╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝╚══════╝╚═╝  ╚═╝
     MICROSERVICES — HEALTHCARE BACKEND PLATFORM
```

[![Java](https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.x-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-3.x-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![gRPC](https://img.shields.io/badge/gRPC-Protobuf-244c5a?style=for-the-badge&logo=grpc&logoColor=white)](https://grpc.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com/)
[![AWS](https://img.shields.io/badge/AWS-LocalStack-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://localstack.cloud/)
[![JWT](https://img.shields.io/badge/JWT-Auth-000000?style=for-the-badge&logo=json-web-tokens&logoColor=white)](https://jwt.io/)

<br/>

> **A production-grade, cloud-ready healthcare backend platform** built with Java Spring Boot and a polyglot microservices architecture — showcasing real-world distributed systems design: gRPC inter-service communication, Kafka event streaming, JWT auth via an API Gateway, AWS infrastructure-as-code with CloudFormation, and comprehensive integration testing.

</div>

---

## 📋 Table of Contents

1. [Architecture Overview](#-architecture-overview)
2. [Services Breakdown](#-services-breakdown)
3. [Technology Stack](#-technology-stack)
4. [Key Engineering Decisions](#-key-engineering-decisions)
5. [Data Flow & Communication Patterns](#-data-flow--communication-patterns)
6. [Infrastructure & Deployment](#-infrastructure--deployment)
7. [API Reference](#-api-reference)
8. [Getting Started](#-getting-started)
9. [Integration Testing](#-integration-testing)
10. [Configuration Reference](#-configuration-reference)

---

## 🏗 Architecture Overview

Healer is a **domain-driven, event-driven microservices system** that manages the full lifecycle of patient data in a healthcare context. Each service owns its domain, its database, and its contracts — with zero shared state between services.

```
                              ┌──────────────────────────────────────┐
                              │           EXTERNAL CLIENTS            │
                              │  (Postman / Frontend / Mobile Apps)   │
                              └───────────────┬──────────────────────┘
                                              │ HTTPS
                                              ▼
                          ┌───────────────────────────────────┐
                          │           API GATEWAY              │
                          │   Spring Cloud Gateway (Port 4004) │
                          │                                    │
                          │  ┌──────────────────────────────┐  │
                          │  │   JWT Validation Filter       │  │
                          │  │   Route-based Auth Guard      │  │
                          │  │   Request Routing & Proxying  │  │
                          │  └──────────────────────────────┘  │
                          └──────┬──────────────┬──────────────┘
                                 │              │
               ┌─────────────────┘              └─────────────────┐
               │  REST                                   REST      │
               ▼                                                   ▼
  ┌────────────────────────┐                     ┌────────────────────────┐
  │    PATIENT SERVICE      │                     │      AUTH SERVICE       │
  │    (Port 8080/5005)     │                     │      (Port 8081)        │
  │                         │                     │                         │
  │  • Patient CRUD         │                     │  • User Registration    │
  │  • Input Validation     │◄─── gRPC ──────────►│  • JWT Login/Logout    │
  │  • Kafka Producer       │    (Port 9005)       │  • BCrypt Password      │
  │  • PostgreSQL           │                     │  • Role-based Access    │
  │  • Remote Debugging     │                     │  • PostgreSQL           │
  └───────────┬─────────────┘                     └────────────────────────┘
              │
              │ Kafka Events (patient.created topic)
              │ [Protobuf Serialized Payload]
              ▼
   ┌──────────────────────┐
   │     APACHE KAFKA      │
   │  (Ports 9092/9094)    │
   │                       │
   │  • KRaft Mode (no ZK) │
   │  • 3 listener config  │
   └───────┬───────────────┘
           │
    ┌──────┴──────────┐
    │                  │
    ▼                  ▼
┌───────────────┐  ┌───────────────────┐
│  ANALYTICS    │  │  NOTIFICATION      │
│  SERVICE      │  │  SERVICE           │
│               │  │                    │
│ • Kafka       │  │ • Kafka Consumer   │
│   Consumer    │  │ • Email Alerts     │
│ • Event       │  │ • Protobuf Decode  │
│   Aggregation │  │                    │
└───────────────┘  └───────────────────┘

              gRPC (billing.proto)
  Patient ──────────────────────────► BILLING SERVICE
  Service                              (Port 9005 gRPC)
                                       • Account Creation
                                       • Proto-defined Contract
                                       • Netty Transport
```

### Design Principles

| Principle | Implementation |
|---|---|
| **Database-per-Service** | Each service has its own isolated PostgreSQL instance — no cross-DB joins |
| **Event-Driven Decoupling** | Kafka pub/sub ensures services evolve independently; producers don't know consumers |
| **Contract-First API Design** | Protobuf schemas define service contracts before implementation |
| **Security at the Edge** | JWT validation happens once at the Gateway; downstream services trust validated claims |
| **Infrastructure as Code** | CloudFormation templates provision all AWS resources (ECS, MSK, RDS) declaratively |

---

## 🔧 Services Breakdown

### 1. 🔐 Auth Service

> **Responsibility:** Identity, authentication, and token lifecycle management

The Auth Service is the **trust anchor** of the system. It issues signed JWTs that downstream services and the API Gateway use for authorization — following the principle of centralized authentication with distributed authorization.

**Key Technical Details:**
- `Spring Security` filter chain with stateless session management
- `BCrypt` password hashing (cost factor 12) — computationally resistant to brute-force
- `JJWT 0.12.6` for RS256/HS256 JWT signing, parsing, and validation
- `SpringDoc OpenAPI 2.6.0` auto-generates Swagger UI at `/swagger-ui.html`
- Database-seeded admin user via `data.sql` for zero-friction bootstrapping
- `H2` in-memory DB for unit/integration tests — no external dependencies needed for CI

**Dependencies:** `spring-boot-starter-security`, `spring-boot-starter-data-jpa`, `jjwt-api`, `postgresql`, `springdoc-openapi-starter-webmvc-ui`

**Endpoints:**
```
POST /auth/login       → Returns signed JWT
POST /auth/register    → Creates user account
GET  /auth/validate    → Validates token (called by Gateway)
```

**Auth Flow:**
```
Client → POST /auth/login (email + password)
       → Auth Service validates credentials against PostgreSQL
       → BCrypt.matches(rawPassword, hashedPassword)
       → Signs JWT with secret key (role embedded as claim)
       → Returns { token: "eyJ..." }
```

---

### 2. 🏥 Patient Service

> **Responsibility:** Patient record management — the core domain of the platform

This is the **system's primary write path**. When a patient is created, it simultaneously triggers two downstream workflows: a synchronous gRPC call to Billing (account creation) and an async Kafka event for Analytics and Notifications.

**Key Technical Details:**
- Full CRUD over patient entities with Bean Validation (`@Valid`, `@NotNull`, `@Email`)
- **Dual communication pattern**: gRPC (sync, for billing) + Kafka (async, for events)
- Publishes Protobuf-serialized events to `patient.created` Kafka topic
- Remote debugging enabled via JDWP on port `5005` — attach IntelliJ debugger at runtime
- `spring.jpa.hibernate.ddl-auto=update` — schema migrations handled by Hibernate
- `SPRING_SQL_INIT_MODE=always` — data seeding on every startup

**gRPC Client Configuration:**
```yaml
billing.service.address: billing-service
billing.service.grpc.port: 9005
```

**Patient Creation Flow:**
```
POST /api/patients
  │
  ├─► Validate DTO (Bean Validation)
  ├─► Persist to PostgreSQL
  ├─► gRPC stub → BillingService.createBillingAccount(patientId, name, email)
  └─► KafkaTemplate.send("patient.created", PatientEvent.toByteArray())
```

---

### 3. 💳 Billing Service

> **Responsibility:** Financial account management, exposed exclusively via gRPC

The Billing Service deliberately **has no REST API** — it's only reachable via gRPC. This enforces a strict service boundary: patient billing logic is decoupled from HTTP concerns and is only callable by authenticated internal services that know the proto contract.

**Key Technical Details:**
- `grpc-spring-boot-starter 3.1.0` for annotation-driven gRPC server (`@GrpcService`)
- Protobuf 4.29.1 schema defines the service contract in `billing.proto`
- `os-maven-plugin` ensures cross-platform `protoc` compilation (Windows/Mac/Linux)
- `protobuf-maven-plugin 0.6.1` auto-generates Java stubs from `.proto` at build time
- `grpc-netty-shaded` — production-grade async HTTP/2 transport

**Proto Contract:**
```protobuf
service BillingService {
  rpc CreateBillingAccount(BillingRequest) returns (BillingResponse);
}

message BillingRequest {
  string patient_id = 1;
  string name = 2;
  string email = 3;
}
```

**Why gRPC here?**
- Strongly-typed contracts prevent integration drift between services
- HTTP/2 multiplexing reduces connection overhead in high-throughput scenarios
- Protobuf serialization is ~3-10x smaller and faster than JSON
- Bi-directional streaming capability for future real-time billing updates

---

### 4. 📊 Analytics Service

> **Responsibility:** Asynchronous event consumption and healthcare data aggregation

The Analytics Service is a **pure Kafka consumer** — it has no inbound HTTP API and never writes to another service's database. This makes it trivially scalable and independently deployable.

**Key Technical Details:**
- `spring-kafka` consumer subscribed to `patient.created` topic
- Protobuf deserialization with `ByteArrayDeserializer` (byte-exact schema enforcement)
- Stateless processing — safe to run multiple instances for throughput scaling
- Consumer group isolation ensures analytics events don't interfere with notification events

---

### 5. 🔔 Notification Service

> **Responsibility:** Event-driven patient notification and alerting

Shares the Kafka topic with Analytics but operates in its own **consumer group** — both services receive every event independently. The Notification Service processes events to trigger emails, SMS, or other communication channels.

**Key Technical Details:**
- `spring-kafka` with Protobuf deserializer
- Consumer group ID isolation ensures separate offset tracking from Analytics
- Protobuf schema reuse from a shared `.proto` definition — single source of truth
- Designed for extension: swap email provider without touching Patient Service

---

### 6. 🚪 API Gateway

> **Responsibility:** Single ingress point, JWT enforcement, and intelligent routing

The Gateway is the **only publicly exposed service**. All traffic enters through port `4004`. It validates JWTs before proxying requests, so downstream services never receive unauthenticated traffic.

**Key Technical Details:**
- `Spring Cloud Gateway` (reactive, built on Project Reactor / Netty)
- Custom `JwtValidationFilter` inspects `Authorization: Bearer <token>` header
- Route predicates map URL patterns to upstream service URLs
- Stateless — horizontally scalable with zero session affinity needed

**Routing Table:**
```yaml
routes:
  - id: patient-service
    uri: http://patient-service:8080
    predicates:
      - Path=/api/patients/**

  - id: auth-service
    uri: http://auth-service:8081
    predicates:
      - Path=/auth/**
```

---

## 🛠 Technology Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| **Language** | Java | 21 | LTS, virtual threads, modern syntax |
| **Framework** | Spring Boot | 3.x | Service scaffolding, DI, auto-config |
| **API Gateway** | Spring Cloud Gateway | 4.x | Reactive routing, JWT filter |
| **Security** | Spring Security + JJWT | 0.12.6 | Auth, BCrypt, JWT lifecycle |
| **Sync RPC** | gRPC + Protobuf | 1.69.0 / 4.29.1 | Type-safe inter-service calls |
| **Async Messaging** | Apache Kafka | 3.x (KRaft) | Event streaming, decoupled pub/sub |
| **Persistence** | PostgreSQL | 16 | Per-service relational databases |
| **ORM** | Spring Data JPA + Hibernate | 6.x | Repository pattern, schema management |
| **API Docs** | SpringDoc OpenAPI | 2.6.0 | Auto-generated Swagger UI |
| **Containerization** | Docker + Docker Compose | 25.x | Local orchestration |
| **Cloud Infra** | AWS (LocalStack) + CloudFormation | — | ECS, MSK, RDS — IaC |
| **Build** | Maven | 3.9.x | Dependency management, protoc plugin |
| **Testing** | JUnit 5 + Spring Test | — | Integration test suite |

---

## 💡 Key Engineering Decisions

### Why gRPC for Billing (not REST)?

Billing is an **internal-only service** — no external consumer should ever call it directly. Using gRPC enforces this at the protocol level (no HTTP REST endpoint exists). Additionally:

- Proto contracts act as a **living API contract** — compilation fails if schemas drift
- Performance: Protobuf binary encoding vs. JSON text is substantially more efficient for high-volume billing operations
- Future: streaming RPCs enable real-time billing event pushes without polling

### Why Kafka for Patient Events (not REST callbacks)?

Patient creation triggers **multiple downstream concerns** (analytics, notifications, potentially audit logging). Synchronous REST callbacks would:
1. Create temporal coupling — Patient Service blocks until all consumers respond
2. Require Patient Service to know all consumers (violates Open/Closed Principle)
3. Fail atomically if any consumer is down

Kafka solves all three: fire-and-forget from Producer, independent consumer group scaling, and consumer replay from offset in case of failures.

### Why KRaft Mode for Kafka?

Kafka 3.x KRaft mode **eliminates ZooKeeper** — reducing operational complexity from two systems to one. The multi-listener config (`PLAINTEXT://kafka:9092`, `EXTERNAL://localhost:9094`) allows both Docker-internal and host-machine connections simultaneously.

### Why Separate PostgreSQL per Service?

Shared databases are the #1 microservice antipattern — they create invisible coupling between services. Each service's schema evolves independently, preventing "one big schema" migrations from cascading across teams.

---

## 🔄 Data Flow & Communication Patterns

### Pattern 1: Synchronous Request (REST + gRPC)

```
Client Request → API Gateway → JWT Validation → Patient Service
                                                     │
                                                     ├─ [sync] gRPC → Billing Service
                                                     │                  └─ Create billing account
                                                     │                  └─ Return BillingResponse
                                                     │
                                                     └─ HTTP 201 Response to Client
```

### Pattern 2: Asynchronous Event Fan-out

```
Patient Service → Kafka (patient.created) ──┬──► Analytics Service (consumer-group: analytics)
                  [Protobuf bytes]           │
                                             └──► Notification Service (consumer-group: notifications)
```

### Pattern 3: Authentication Flow

```
Client → POST /auth/login → Auth Service → JWT signed response
Client → GET /api/patients (Bearer token) → API Gateway → JwtFilter.validate()
                                                             ├─ Valid: proxy to Patient Service
                                                             └─ Invalid: 401 Unauthorized
```

---

## 🚀 Infrastructure & Deployment

### Local Development (Docker Compose)

The `infrastructure/` directory contains Docker Compose configurations that wire all services together with proper networking, environment variables, and health checks.

```bash
# Start all infrastructure (Kafka, Postgres instances, services)
docker compose -f infrastructure/docker-compose.yml up -d

# View logs for a specific service
docker compose logs -f patient-service

# Remote debug patient-service (attach on port 5005)
# JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```

### AWS Cloud Deployment (LocalStack + CloudFormation)

The `infrastructure/` directory also contains **CloudFormation templates** that provision:

| AWS Resource | Purpose |
|---|---|
| **Amazon ECS (Fargate)** | Serverless container orchestration — no EC2 management |
| **Amazon MSK** | Managed Kafka — replaces self-hosted Kafka in production |
| **Amazon RDS (PostgreSQL)** | Managed, multi-AZ PostgreSQL for each service |
| **Application Load Balancer** | Replaces the Docker-based API Gateway in cloud |
| **VPC + Security Groups** | Network isolation between services |

```bash
# Deploy to LocalStack (simulates AWS locally)
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/main.yml \
  --stack-name healer-stack \
  --endpoint-url http://localhost:4566

# Test against LocalStack
export AWS_ENDPOINT=http://localhost:4566
```

### Docker Network Architecture

```
healer-network (bridge)
    ├── patient-service          :8080 (HTTP), :5005 (JDWP)
    ├── patient-service-db       :5432
    ├── auth-service             :8081
    ├── auth-service-db          :5432
    ├── billing-service          :9005 (gRPC)
    ├── analytics-service
    ├── notification-service
    ├── kafka                    :9092 (internal), :9094 (external)
    └── api-gateway              :4004 (public ingress)
```

---

## 📡 API Reference

All requests go through the API Gateway at `http://localhost:4004`.

### Authentication

```bash
# Login
POST /auth/login
Content-Type: application/json

{
  "email": "testuser@test.com",
  "password": "password"
}

# Response
{
  "token": "eyJhbGciOiJIUzI1NiJ9..."
}
```

### Patient Endpoints (Requires JWT)

```bash
# Get all patients
GET /api/patients
Authorization: Bearer <token>

# Create patient
POST /api/patients
Authorization: Bearer <token>
Content-Type: application/json

{
  "name": "Jane Doe",
  "email": "jane@example.com",
  "address": "123 Health St",
  "dateOfBirth": "1990-05-15"
}

# Update patient
PUT /api/patients/{id}
Authorization: Bearer <token>

# Delete patient
DELETE /api/patients/{id}
Authorization: Bearer <token>
```

### HTTP Status Codes

| Code | Meaning |
|---|---|
| `200` | OK — successful GET/PUT |
| `201` | Created — patient created, billing account provisioned |
| `401` | Unauthorized — missing or invalid JWT |
| `403` | Forbidden — insufficient role/permissions |
| `422` | Unprocessable Entity — Bean Validation failure |

---

## ⚡ Getting Started

### Prerequisites

```bash
java --version    # Java 21+
mvn --version     # Maven 3.9+
docker --version  # Docker 25+
```

### 1. Clone & Build

```bash
git clone https://github.com/ArnavBuild04/HealerMicroservices.git
cd HealerMicroservices

# Build all services (skipping tests for speed)
mvn clean install -DskipTests
```

### 2. Start Infrastructure

```bash
cd infrastructure
docker compose up -d

# Verify all services are healthy
docker compose ps
```

### 3. Seed Auth Database

The Auth Service uses `data.sql` to create a default admin user on startup:

```
Email:    testuser@test.com
Password: password
Role:     ADMIN
```

### 4. Test the Full Flow

```bash
# Step 1: Authenticate
TOKEN=$(curl -s -X POST http://localhost:4004/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"testuser@test.com","password":"password"}' \
  | jq -r '.token')

# Step 2: Create a patient (triggers gRPC + Kafka)
curl -X POST http://localhost:4004/api/patients \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Smith",
    "email": "john@healer.dev",
    "address": "456 Wellness Ave",
    "dateOfBirth": "1985-03-22"
  }'

# Step 3: Verify in Kafka (check analytics-service logs)
docker compose logs analytics-service
```

### 5. Remote Debugging (Patient Service)

```bash
# Attach IntelliJ debugger to port 5005
# Run → Edit Configurations → Remote JVM Debug → Port 5005
# JDWP is pre-configured via JAVA_TOOL_OPTIONS env var
```

---

## 🧪 Integration Testing

The `integration-tests/` module contains end-to-end tests that validate the **entire request lifecycle** across service boundaries — not just unit-level logic.

```bash
# Run integration tests (requires Docker Compose running)
cd integration-tests
mvn verify

# Test coverage includes:
# ✓ Auth token issuance and validation
# ✓ Gateway JWT enforcement (401 on invalid tokens)
# ✓ Patient CRUD through the Gateway
# ✓ Kafka event publishing on patient creation
# ✓ gRPC billing account creation
```

### Test Architecture

```
integration-tests/
  └── src/test/java/
        ├── AuthIntegrationTest.java      # Login, register, token validation
        ├── PatientIntegrationTest.java   # Full CRUD via Gateway
        ├── BillingGrpcTest.java         # gRPC stub test against billing-service
        └── KafkaEventTest.java          # Verify events published to Kafka
```

---

## ⚙️ Configuration Reference

### Patient Service

```properties
# Database
SPRING_DATASOURCE_URL=jdbc:postgresql://patient-service-db:5432/db
SPRING_DATASOURCE_USERNAME=admin_user
SPRING_DATASOURCE_PASSWORD=password
SPRING_JPA_HIBERNATE_DDL_AUTO=update
SPRING_SQL_INIT_MODE=always

# Kafka
SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# gRPC (Billing)
BILLING_SERVICE_ADDRESS=billing-service
BILLING_SERVICE_GRPC_PORT=9005

# Remote Debug
JAVA_TOOL_OPTIONS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
```

### Auth Service

```properties
SPRING_DATASOURCE_URL=jdbc:postgresql://auth-service-db:5432/db
SPRING_DATASOURCE_USERNAME=admin_user
SPRING_DATASOURCE_PASSWORD=password
SPRING_JPA_HIBERNATE_DDL_AUTO=update
SPRING_SQL_INIT_MODE=always
```

### Notification / Analytics Service

```properties
SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
```

### Kafka (KRaft Mode)

```
KAFKA_CFG_NODE_ID=0
KAFKA_CFG_PROCESS_ROLES=controller,broker
KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT
KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
```

---

## 📁 Project Structure

```
HealerMicroservices/
├── 📁 api-gateway/                   # Spring Cloud Gateway — public ingress
│   └── src/main/java/
│       ├── config/RouteConfig.java   # Route definitions
│       └── filter/JwtFilter.java     # JWT validation filter
│
├── 📁 auth-service/                  # JWT auth, BCrypt, user management
│   └── src/main/
│       ├── java/.../security/        # Spring Security config
│       ├── java/.../controller/      # /auth endpoints
│       └── resources/data.sql        # Seed admin user
│
├── 📁 patient-service/               # Core domain — patient CRUD
│   └── src/main/
│       ├── java/.../controller/      # REST endpoints
│       ├── java/.../grpc/            # gRPC client stub
│       └── java/.../kafka/           # Kafka producer
│
├── 📁 billing-service/               # gRPC server — billing accounts
│   └── src/main/
│       ├── proto/billing.proto       # Service contract
│       └── java/.../grpc/            # @GrpcService implementation
│
├── 📁 analytics-service/             # Kafka consumer — event aggregation
├── 📁 notification-service/          # Kafka consumer — alerts & emails
│
├── 📁 infrastructure/                # All deployment configs
│   ├── docker-compose.yml            # Local orchestration
│   └── cloudformation/               # AWS IaC templates (ECS, MSK, RDS)
│
├── 📁 integration-tests/             # End-to-end test suite
├── 📁 api-requests/                  # Saved HTTP request files (Postman/IntelliJ)
└── 📁 grpc-requests/                 # gRPC test request payloads
    └── billing-service/
```

---

<div align="center">

**Built with ☕ Java, ❤️ Spring Boot, and a genuine passion for distributed systems**

*Healer Microservices — where healthcare meets production-grade backend engineering*

</div>
