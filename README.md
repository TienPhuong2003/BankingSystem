# 🏦 Fraud Detection Digital Banking System

A **production-ready microservices-based banking system** with real-time fraud detection, distributed transactions (SAGA pattern), and event-driven architecture using Kafka. Built to handle complex financial operations securely while detecting suspicious activities in real-time.

![Status](https://img.shields.io/badge/status-active-success)
![Java](https://img.shields.io/badge/Java-21-blue)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.0-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Services](#services)
- [Configuration](#configuration)
- [How to Run](#how-to-run)
- [API Documentation](#api-documentation)
- [Fraud Detection Rules](#fraud-detection-rules)
- [SAGA Pattern Explanation](#saga-pattern-explanation)
- [Workflow Examples](#workflow-examples)
- [Testing](#testing)
- [Monitoring & Debugging](#monitoring--debugging)
- [Key Concepts](#key-concepts)

---

## 🎯 Overview

### What is This?

A **distributed banking system** that safely processes financial transactions between users while automatically detecting fraudulent activities in real-time using intelligent pattern recognition.

### Key Features

✅ **Real-Time Fraud Detection**
- Velocity checks (too many transactions)
- Amount anomaly detection (unusual transaction sizes)
- Balance percentage validation
- Immediate account blocking on suspicious activity

✅ **SAGA Pattern Implementation**
- Distributed transactions across microservices
- Automatic compensation/refunds on failure
- Consistent state management without database locks

✅ **Event-Driven Architecture**
- Asynchronous processing via Apache Kafka
- Decoupled microservices
- Scalable and resilient

✅ **Multi-Service Orchestration**
- Account Management
- Payment Processing (Razorpay integration)
- Transaction Handling
- Fraud Detection
- Notifications (OTP + SMS alerts)

✅ **Security & Compliance**
- OTP verification for suspicious transactions
- Account blocking on fraud detection
- Complete transaction audit trail
- Automatic refunds with compensation

---

## 🛠️ Tech Stack

### Core Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Language** | Java | 21 LTS | Backend development |
| **Framework** | Spring Boot | 4.1.0 | Microservices foundation |
| **Cloud** | Spring Cloud | 2025.1.2 | Service-to-service communication |
| **Message Broker** | Apache Kafka | Latest | Event streaming |
| **Cache** | Redis | Latest | Fraud rule evaluation |
| **Database** | MySQL | 8.0+ | Persistent storage |
| **Payment Gateway** | Razorpay | v1 | Payment processing |
| **API Gateway** | Spring Cloud Gateway | 2025.1.2 | Request routing & caching |
| **Build Tool** | Maven | 3.9.16+ | Dependency management |

### Key Dependencies

```xml
<!-- Spring Boot Starters -->
<spring-boot-starter-web/>
<spring-boot-starter-data-jpa/>
<spring-boot-starter-kafka/>
<spring-boot-starter-actuator/>
<spring-boot-starter-validation/>

<!-- Spring Cloud -->
<spring-cloud-starter-gateway-server-webflux/>
<spring-cloud-starter-openfeign/>

<!-- Database & Cache -->
<mysql-connector-j/>
<spring-boot-starter-data-redis/>

<!-- Utilities -->
<lombok/>
<razorpay-java/>
```

---

## 🏗️ Architecture

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL CLIENTS/APPS                               │
│                   (Mobile App, Web Browser, API Clients)                    │
└──────────────────────────────┬──────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────────────────────┐
        │                  API GATEWAY (Port 8080)                │
        │         Spring Cloud Gateway + Redis Cache              │
        │  • Request routing to microservices                     │
        │  • Load balancing                                        │
        │  • Response caching                                      │
        └────────────────┬─────────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┬──────────────┬──────────────┐
        │                │                │              │              │
        ▼                ▼                ▼              ▼              ▼
   ┌──────────┐    ┌──────────┐    ┌──────────┐  ┌──────────┐  ┌──────────┐
   │ Account  │    │ Payment  │    │Transaction│  │ Fraud    │  │Notification│
   │ Service  │    │ Service  │    │ Service  │  │Detection │  │  Service   │
   │(8081)    │    │(8082)    │    │(8083)    │  │ (8084)   │  │  (8085)    │
   └────┬─────┘    └────┬─────┘    └────┬─────┘  └────┬─────┘  └────┬───────┘
        │               │               │             │             │
        │               │               │             │             │
    ┌───▼─────────────────────────────────────────────────────────────▼───┐
    │                      KAFKA BROKER (Message Broker)                  │
    │                                                                      │
    │  Topics:                                                           │
    │  • transaction.initiated      → Fraud Detection                   │
    │  • verification.required      → Notification Service              │
    │  • fraud.check.clean          → Transaction Service               │
    │  • transaction.completed      → Account, Notification             │
    │  • fraud.detected             → Account, Notification             │
    │  • transaction.refunded       → Notification                      │
    │  • payment.completed          → Account, Notification             │
    │  • payment.failed             → Transaction, Notification         │
    │                                                                      │
    └───┬─────────────────────────────────────────────────────────────┬───┘
        │                                                             │
        │                                                             │
    ┌───▼──────────────────┐                            ┌───────────▼──────┐
    │   MySQL Databases    │                            │  Redis Cache     │
    │                      │                            │                  │
    │ • account_db         │                            │ Fraud Rules:     │
    │ • payment_db         │                            │ • Velocity check │
    │ • transaction_db     │                            │ • Amount check   │
    │ • notification_db    │                            │ • Balance check  │
    │                      │                            │ • OTP storage    │
    └──────────────────────┘                            └──────────────────┘
```

### Microservices Communication Pattern

```
Synchronous (Direct Calls):
  Account Service ←→ Fraud Detection Service
  Transaction Service ←→ Account Service
  Payment Service ←→ Account Service

Asynchronous (Event-Driven):
  TransactionService → Kafka → FraudDetectionService
  FraudDetectionService → Kafka → TransactionService
  TransactionService → Kafka → AccountService
  Any Service → Kafka → NotificationService
```

---

## 🔧 Services

### 1. **Account Service** (Port 8081)

**Purpose:** Manages user bank accounts, balance operations, and account states

**Key Responsibilities:**
- Create and manage bank accounts
- Track account balances (SAVINGS, CURRENT, FIXED_DEPOSIT)
- Handle debit/credit operations
- Block accounts on fraud detection
- Set daily transaction limits

**Technologies:**
- Spring Boot Web + Data JPA
- MySQL
- Kafka Consumer

**Endpoints:**
```
POST   /api/v1/accounts                    → Create account
GET    /api/v1/accounts/{accountNumber}    → Get account details
GET    /api/v1/accounts/{accountNumber}/balance → Get balance
PUT    /api/v1/accounts/{accountNumber}/deduct   → Deduct balance
PUT    /api/v1/accounts/{accountNumber}/credit   → Credit balance
PUT    /api/v1/accounts/{accountNumber}/block    → Block account
```

**Kafka Listeners:**
- `transaction.completed` → Credit receiver's account
- `fraud.detected` → Block account

---

### 2. **Transaction Service** (Port 8083)

**Purpose:** Orchestrates money transfers using SAGA pattern

**Key Responsibilities:**
- Initiate money transfers (SAGA orchestrator)
- Manage transaction states (PROCESSING → COMPLETED or FLAGGED)
- Handle OTP verification
- Implement SAGA compensation (automatic refunds)
- Track transaction history

**Technologies:**
- Spring Boot Web + Data JPA
- Kafka Producer/Consumer
- Redis (OTP storage)

**Endpoints:**
```
POST   /api/v1/transactions/transfer                    → Initiate transfer
POST   /api/v1/transactions/{txId}/verify-otp          → Verify OTP
GET    /api/v1/transactions/{txId}                     → Get transaction
GET    /api/v1/transactions/history/{accountNumber}    → Get history
```

**Kafka Publishers:**
- `transaction.initiated` (fraud check)
- `transaction.completed` (success path)
- `fraud.detected` + `transaction.refunded` (failure path)

---

### 3. **Fraud Detection Service** (Port 8084)

**Purpose:** Real-time fraud detection using Redis-based pattern matching

**Key Responsibilities:**
- Evaluate 3 fraud patterns:
  1. **Velocity Check:** Max 5 transactions per 60 seconds
  2. **Amount Check:** Amount < 5x average transaction
  3. **Balance Check:** Amount < 90% of account balance
- Cache fraud metrics in Redis
- Determine if transaction needs OTP verification
- Support legitimate high-value transactions

**Technologies:**
- Spring Boot Web
- Kafka Consumer
- Redis (fraud metrics)
- OpenFeign (Account Service calls)

**Configuration:**
```yaml
fraud:
  max-transactions-per-minute: 5
  suspicious-amount-multiplier: 5
  max-balance-percentage: 0.90
```

**Kafka Listeners:**
- `transaction.initiated` → Perform fraud checks

**Kafka Publishers:**
- `verification.required` (suspicious)
- `fraud.check.clean` (legitimate)

---

### 4. **Payment Service** (Port 8082)

**Purpose:** Razorpay payment gateway integration

**Key Responsibilities:**
- Create payment orders
- Handle Razorpay webhooks
- Track payment status
- Publish payment completion/failure events

**Technologies:**
- Spring Boot Web + Data JPA
- Razorpay Java SDK
- Kafka Producer

**Endpoints:**
```
POST   /api/v1/payments/order              → Create payment order
POST   /api/v1/payments/webhook            → Handle Razorpay webhook
```

**Kafka Publishers:**
- `payment.completed` (payment captured)
- `payment.failed` (payment failed)

---

### 5. **Notification Service** (Port 8085)

**Purpose:** Multi-channel notifications (SMS, Email alerts)

**Key Responsibilities:**
- Send OTP via SMS
- Send transaction alerts (debit/credit)
- Send fraud alerts
- Send refund notifications
- Send payment status updates

**Technologies:**
- Spring Boot Web
- Kafka Consumer

**Kafka Listeners:**
- `transaction.otp.generated` → Send OTP
- `transaction.completed` → Send transaction alerts
- `fraud.detected` → Send fraud alert
- `transaction.refunded` → Send refund alert
- `payment.completed` → Send payment success
- `payment.failed` → Send payment failure

**Implementation Note:** Currently logs alerts. Integrate with SMS provider (Twilio, AWS SNS) for production.

---

### 6. **API Gateway** (Port 8080)

**Purpose:** Single entry point for all client requests

**Key Responsibilities:**
- Route requests to appropriate microservices
- Implement Redis-based response caching
- Load balancing
- Circuit breaker pattern

**Technologies:**
- Spring Cloud Gateway
- Spring Cloud LoadBalancer
- Spring Data Redis

**Configuration:**
```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: account-service
          uri: http://localhost:8081
          predicates:
            - Path=/api/v1/accounts/**
        - id: transaction-service
          uri: http://localhost:8083
          predicates:
            - Path=/api/v1/transactions/**
        # ... more routes
```

---

## ⚙️ Configuration

### Database Setup

#### Account Service (MySQL)
```sql
CREATE DATABASE account_db;
USE account_db;

-- Tables auto-created by Hibernate
-- Fields:
-- - id (UUID)
-- - accountNumber (unique, 12 digits)
-- - accountHolderName
-- - email (unique)
-- - phone
-- - accountType (ENUM: SAVINGS, CURRENT, FIXED_DEPOSIT)
-- - status (ENUM: ACTIVE, BLOCKED, CLOSED)
-- - balance (DECIMAL)
-- - dailyTransactionLimit (DECIMAL)
-- - createdAt, updatedAt (timestamps)
```

#### Transaction Service (MySQL)
```sql
CREATE DATABASE transaction_db;
USE transaction_db;

-- Fields:
-- - id (UUID)
-- - senderAccountNumber
-- - receiverAccountNumber
-- - amount (DECIMAL)
-- - status (ENUM: PROCESSING, COMPLETED, FLAGGED, REFUNDED)
-- - type (ENUM: TRANSFER)
-- - referenceNumber (unique)
-- - failureReason (optional)
-- - createdAt, completedAt (timestamps)
```

#### Payment Service (MySQL)
```sql
CREATE DATABASE payment_db;
USE payment_db;

-- Fields:
-- - id (UUID)
-- - razorpayOrderId (unique)
-- - accountNumber
-- - amount (DECIMAL)
-- - currency (INR)
-- - status (ENUM: CREATED, COMPLETED, FAILED)
-- - description
-- - failureReason (optional)
-- - createdAt (timestamp)
```

### Environment Variables

Create `.env` file in root directory:

```bash
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=root

# Kafka Configuration
KAFKA_BROKER=localhost:9092

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379

# Razorpay Configuration (get from dashboard)
RAZORPAY_KEY_ID=your_razorpay_key_id
RAZORPAY_KEY_SECRET=your_razorpay_key_secret
RAZORPAY_CURRENCY=INR
```

### Service Configuration Files

#### Account Service (`account-service/src/main/resources/application.yaml`)
```yaml
spring:
  application:
    name: account-service
  datasource:
    url: jdbc:mysql://localhost:3306/account_db?createDatabaseIfNotExist=true
    username: root
    password: root
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.MySQLDialect
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: account-service-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.package: "*"
        spring.json.value.default.type: java.util.HashMap

server:
  port: 8081

management:
  endpoints:
    web:
      exposure:
        include: health,info
```

#### Fraud Detection Service (`fraud-detection-service/src/main/resources/application.yaml`)
```yaml
spring:
  application:
    name: fraud-detection-service
  data:
    redis:
      host: localhost
      port: 6379
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: fraud-detection-group
      auto-offset-reset: earliest

server:
  port: 8084

fraud:
  max-transactions-per-minute: 5
  suspicious-amount-multiplier: 5
  max-balance-percentage: 0.90

management:
  endpoints:
    web:
      exposure:
        include: health,info
```

#### Transaction Service (`transaction-service/src/main/resources/application.yaml`)
```yaml
spring:
  application:
    name: transaction-service
  datasource:
    url: jdbc:mysql://localhost:3306/transaction_db?createDatabaseIfNotExist=true
    username: root
    password: root
  jpa:
    hibernate:
      ddl-auto: update
  data:
    redis:
      host: localhost
      port: 6379
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: transaction-service-group

server:
  port: 8083
```

#### Payment Service (`payment-service/src/main/resources/application.yaml`)
```yaml
spring:
  application:
    name: payment-service
  datasource:
    url: jdbc:mysql://localhost:3306/payment_db?createDatabaseIfNotExist=true
    username: root
    password: root
  jpa:
    hibernate:
      ddl-auto: update
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer

server:
  port: 8082

razorpay:
  key-id: ${RAZORPAY_KEY_ID}
  key-secret: ${RAZORPAY_KEY_SECRET}
  currency: INR
```

---

## 🚀 How to Run

### Prerequisites

- **Java 21 LTS** or higher
- **Maven 3.9.16** or higher
- **MySQL 8.0** or higher
- **Apache Kafka 3.0** or higher
- **Redis 6.0** or higher
- **Git**

### Step 1: Install Dependencies

#### On macOS (using Homebrew)
```bash
# Install Java 21
brew install openjdk@21

# Install MySQL
brew install mysql

# Install Kafka
brew install kafka

# Install Redis
brew install redis
```

#### On Ubuntu/Debian
```bash
# Install Java 21
sudo apt-get install openjdk-21-jdk

# Install MySQL
sudo apt-get install mysql-server

# Install Kafka
# Download from https://kafka.apache.org/downloads

# Install Redis
sudo apt-get install redis-server
```

#### On Windows
- Download Java 21 from [oracle.com](https://www.oracle.com/java/technologies/downloads/#java21)
- Download MySQL from [mysql.com](https://dev.mysql.com/downloads/mysql/)
- Download Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
- Download Redis from [github.com/microsoftarchive/redis](https://github.com/microsoftarchive/redis/releases)

### Step 2: Start Infrastructure

#### Start MySQL
```bash
# macOS
brew services start mysql

# Linux
sudo systemctl start mysql

# Windows (run as Administrator)
net start MySQL80
```

#### Start Kafka
```bash
# Terminal 1: Start Zookeeper
cd kafka_install_path
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

#### Start Redis
```bash
# macOS/Linux
redis-server

# Windows
redis-server.exe
```

### Step 3: Clone Repository

```bash
git clone https://github.com/TienPhuong2003/BankingSystem.git
cd BankingSystem
```

### Step 4: Build Project

```bash
# Build entire project (all services)
mvn clean install

# Build individual service
cd account-service
mvn clean install
```

### Step 5: Start Services

**Important:** Start services in this order (each in separate terminal)

#### Terminal 1: Start Account Service
```bash
cd account-service
./mvnw spring-boot:run
# or
mvn spring-boot:run
```

#### Terminal 2: Start Fraud Detection Service
```bash
cd fraud-detection-service
./mvnw spring-boot:run
```

#### Terminal 3: Start Transaction Service
```bash
cd transaction-service
./mvnw spring-boot:run
```

#### Terminal 4: Start Payment Service
```bash
cd payment-service
./mvnw spring-boot:run
```

#### Terminal 5: Start Notification Service
```bash
cd notification-service
./mvnw spring-boot:run
```

#### Terminal 6: Start API Gateway
```bash
cd api-gateway
./mvnw spring-boot:run
```

### Step 6: Verify All Services Running

```bash
# Check all ports are listening
lsof -i :8080 :8081 :8082 :8083 :8084 :8085

# or test via curl
curl http://localhost:8080/api/v1/accounts
curl http://localhost:8081/actuator/health
curl http://localhost:8083/actuator/health
# etc.
```

### Using Docker (Optional)

Create `docker-compose.yml` at root:

```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  redis:
    image: redis:7.0
    ports:
      - "6379:6379"

volumes:
  mysql_data:
```

Run with Docker:
```bash
docker-compose up -d

# Check logs
docker-compose logs -f

# Stop all services
docker-compose down
```

---

## 📚 API Documentation

### Base URLs
- **API Gateway:** `http://localhost:8080`
- **Account Service:** `http://localhost:8081`
- **Payment Service:** `http://localhost:8082`
- **Transaction Service:** `http://localhost:8083`
- **Fraud Detection:** `http://localhost:8084`

### Account Service APIs

#### 1. Create Account
```bash
curl -X POST http://localhost:8081/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountHolderName": "Alice Johnson",
    "email": "alice@example.com",
    "phone": "9876543210",
    "accountType": "SAVINGS",
    "initialDeposit": 100000.00
  }'
```

**Response:**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "accountNumber": "123456789012",
  "accountHolderName": "Alice Johnson",
  "email": "alice@example.com",
  "phone": "9876543210",
  "accountType": "SAVINGS",
  "status": "ACTIVE",
  "balance": 100000.00,
  "dailyTransactionLimit": 100000.00,
  "createdAt": "2024-09-10T15:30:00Z",
  "updatedAt": "2024-09-10T15:30:00Z"
}
```

#### 2. Get Account
```bash
curl http://localhost:8081/api/v1/accounts/123456789012
```

#### 3. Get Balance
```bash
curl http://localhost:8081/api/v1/accounts/123456789012/balance
# Returns: 100000.00
```

#### 4. Deduct Balance
```bash
curl -X PUT "http://localhost:8081/api/v1/accounts/123456789012/deduct?amount=50000"
```

#### 5. Credit Balance
```bash
curl -X PUT "http://localhost:8081/api/v1/accounts/123456789012/credit?amount=50000"
```

### Transaction Service APIs

#### 1. Initiate Transfer
```bash
curl -X POST http://localhost:8083/api/v1/transactions/transfer \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountNumber": "123456789012",
    "receiverAccountNumber": "987654321098",
    "amount": 50000
  }'
```

**Response:**
```json
{
  "id": "tx_001",
  "senderAccountNumber": "123456789012",
  "receiverAccountNumber": "987654321098",
  "amount": 50000.00,
  "status": "PROCESSING",
  "type": "TRANSFER",
  "referenceNumber": "ref_123456",
  "createdAt": "2024-09-10T15:35:00Z"
}
```

#### 2. Verify OTP
```bash
curl -X POST http://localhost:8083/api/v1/transactions/tx_001/verify-otp \
  -H "Content-Type: application/json" \
  -d '{
    "otp": "123456"
  }'
```

#### 3. Get Transaction
```bash
curl http://localhost:8083/api/v1/transactions/tx_001
```

#### 4. Get Transaction History
```bash
curl http://localhost:8083/api/v1/transactions/history/123456789012
```

### Payment Service APIs

#### 1. Create Payment Order
```bash
curl -X POST http://localhost:8082/api/v1/payments/order \
  -H "Content-Type: application/json" \
  -d '{
    "accountNumber": "123456789012",
    "amount": 5000.00,
    "description": "Online Shopping"
  }'
```

**Response:**
```json
{
  "id": "pay_001",
  "razorpayOrderId": "order_KUH7OHCH8xYkOv",
  "amount": 5000.00,
  "currency": "INR",
  "status": "CREATED",
  "keyId": "rzp_live_xxxxxxxxxxxx"
}
```

---

## 🔍 Fraud Detection Rules

### Rule 1: Velocity Check
**What:** Too many transactions in a short time window

**Config:**
```yaml
fraud:
  max-transactions-per-minute: 5
```

**Logic:**
- Tracks transaction count per account in Redis
- 60-second sliding window
- If count > 5 → Flag as fraud → Require OTP

**Example:**
```
Alice makes 6 transfers in 30 seconds
→ Count: 6 > 5
→ FRAUD DETECTED
→ Send OTP
```

---

### Rule 2: Suspicious Amount
**What:** Transaction amount significantly higher than average

**Config:**
```yaml
fraud:
  suspicious-amount-multiplier: 5
```

**Logic:**
- Tracks running average transaction amount in Redis
- If current amount > (average × multiplier) → Flag as fraud
- Updates average after each transaction

**Example:**
```
Alice's average: ₹10,000
Threshold: ₹10,000 × 5 = ₹50,000
Current transfer: ₹75,000
→ 75,000 > 50,000
→ FRAUD DETECTED
→ Send OTP
```

---

### Rule 3: Balance Check
**What:** Attempting to transfer most of account balance

**Config:**
```yaml
fraud:
  max-balance-percentage: 0.90
```

**Logic:**
- Calculates max allowed amount = balance × percentage
- If transfer > max allowed → Flag as fraud
- Prevents large bulk transfers

**Example:**
```
Alice's balance: ₹60,000
Max allowed: ₹60,000 × 0.90 = ₹54,000
Current transfer: ₹55,000
→ 55,000 > 54,000
→ FRAUD DETECTED
→ Send OTP
```

---

## SAGA Pattern Explanation

### What is SAGA?

SAGA is a **distributed transaction pattern** for microservices that:
- Replaces traditional database transactions (which can't span multiple services)
- Uses a series of **local transactions** with **compensating actions**
- Maintains **eventual consistency** without distributed locks

### How It Works in This System

#### Happy Path (Successful Transfer)

```
Step 1: TransactionService.transfer()
├─ Deduct ₹50,000 from Alice
└─ Publish: transaction.initiated

Step 2: FraudDetectionService
├─ Check all 3 rules
└─ Publish: fraud.check.clean (all passed)

Step 3: TransactionService.processCleanResult()
├─ Update TX: PROCESSING → COMPLETED
└─ Publish: transaction.completed

Step 4: AccountService
├─ Credit ₹50,000 to Bob
└─ Balance updated

Step 5: NotificationService
├─ Send debit alert to Alice
└─ Send credit alert to Bob

✅ SAGA COMPLETED SUCCESSFULLY
Alice: ₹50K, Bob: ₹150K, TX: COMPLETED
```

#### Compensation Path (Wrong OTP)

```
Step 1: TransactionService.transfer()
├─ Deduct ₹50,000 from Alice
└─ Publish: transaction.initiated

Step 2: FraudDetectionService
├─ Detect suspicious amount
└─ Publish: verification.required

Step 3: NotificationService
└─ Send OTP to Alice

Step 4: User enters WRONG OTP

Step 5: TransactionService.verifyOTP()
├─ OTP mismatch detected
└─ Call: blockAccountAndCompensate()

Step 6: 🔄 COMPENSATION TRIGGERED
├─ Publish: fraud.detected
│  └─ AccountService blocks Alice's account
├─ Publish: transaction.refunded
│  └─ Credit ₹50,000 back to Alice
└─ Transaction status: FLAGGED

Step 7: NotificationService
├─ Send fraud alert to Alice
└─ Send refund confirmation

✅ SAGA COMPENSATED SUCCESSFULLY
Alice: ₹100K (refunded!), Bob: ₹100K, TX: FLAGGED, Account: BLOCKED
```

### Key Benefits

✅ **Consistency:** Automatic compensation ensures no lost money
✅ **Visibility:** Every compensation action is explicit in code
✅ **Scalability:** No need to coordinate across database locks
✅ **Resilience:** Service can fail and compensation happens independently

---

## 📊 Workflow Examples

### Example 1: Clean Transfer (No Fraud)

```bash
# Step 1: Create accounts
curl -X POST http://localhost:8081/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountHolderName": "Alice",
    "email": "alice@bank.com",
    "phone": "9000000001",
    "accountType": "SAVINGS",
    "initialDeposit": 100000
  }'
# Response: accountNumber = "111111111111"

curl -X POST http://localhost:8081/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountHolderName": "Bob",
    "email": "bob@bank.com",
    "phone": "9000000002",
    "accountType": "SAVINGS",
    "initialDeposit": 100000
  }'
# Response: accountNumber = "222222222222"

# Step 2: Initiate small transfer (no fraud)
curl -X POST http://localhost:8083/api/v1/transactions/transfer \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountNumber": "111111111111",
    "receiverAccountNumber": "222222222222",
    "amount": 1000
  }'
# Response: tx_id = "tx_clean_001", status = PROCESSING

# Step 3: Check logs
# - Fraud Detection logs: "All checks passed"
# - Transaction completes automatically (no OTP)
# - Account balances updated

# Step 4: Verify result
curl http://localhost:8081/api/v1/accounts/111111111111/balance
# Response: 99000.00

curl http://localhost:8081/api/v1/accounts/222222222222/balance
# Response: 101000.00
```

### Example 2: Fraud Detection & Compensation

```bash
# Assume Alice has ₹100,000 balance

# Step 1: Attempt large transfer (90% of balance)
curl -X POST http://localhost:8083/api/v1/transactions/transfer \
  -H "Content-Type: application/json" \
  -d '{
    "senderAccountNumber": "111111111111",
    "receiverAccountNumber": "222222222222",
    "amount": 90000
  }'
# Response: tx_id = "tx_fraud_001", status = PROCESSING

# Step 2: Check logs
# - Fraud Detection logs: "Transaction exceed 90% of account balance"
# - Publish: verification.required
# - Notification Service logs: "OTP sent to Alice"

# Step 3: User enters WRONG OTP
curl -X POST "http://localhost:8083/api/v1/transactions/tx_fraud_001/verify-otp" \
  -H "Content-Type: application/json" \
  -d '{"otp": "000000"}'

# Response:
# {
#   "status": "FLAGGED",
#   "message": "Wrong OTP entered - account blocked"
# }

# Step 4: Check compensation results
curl http://localhost:8081/api/v1/accounts/111111111111
# Response: status = BLOCKED, balance = 100000.00 (REFUNDED!)

# Step 5: Check transaction
curl http://localhost:8083/api/v1/transactions/tx_fraud_001
# Response: status = FLAGGED, failureReason = "Wrong OTP entered..."
```

### Example 3: Velocity Fraud (Too Many Transactions)

```bash
# Step 1-5: Make 6 rapid transfers in succession
for i in {1..6}; do
  curl -X POST http://localhost:8083/api/v1/transactions/transfer \
    -H "Content-Type: application/json" \
    -d '{
      "senderAccountNumber": "111111111111",
      "receiverAccountNumber": "222222222222",
      "amount": 1000
    }'
  sleep 2
done

# Transaction 6: Check logs
# - Fraud Detection logs: "Too many transactions in 60 seconds"
# - Publish: verification.required
# - OTP sent to Alice

# Step 2: Correct OTP
curl -X POST "http://localhost:8083/api/v1/transactions/tx_velocity_006/verify-otp" \
  -H "Content-Type: application/json" \
  -d '{"otp": "123456"}'
# Response: status = COMPLETED

# Step 3: After 60 seconds, velocity counter resets
```

---

## 🧪 Testing

### Unit Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=AccountServiceTest

# Run with coverage
mvn test jacoco:report
```

### Integration Testing

```bash
# Test fraud detection rules
cd fraud-detection-service
mvn test -Dtest=FraudDetectionServiceTest

# Test SAGA pattern
cd transaction-service
mvn test -Dtest=TransactionServiceSagaTest
```

### Manual API Testing

#### Test Script: `test-api.sh`
```bash
#!/bin/bash

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Testing Banking System APIs..."

# Test 1: Create Account
echo -e "\n${GREEN}[Test 1] Creating Account...${NC}"
ACCOUNT_RESPONSE=$(curl -s -X POST http://localhost:8081/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountHolderName": "Test User",
    "email": "test@example.com",
    "phone": "9000000000",
    "accountType": "SAVINGS",
    "initialDeposit": 100000
  }')

ACCOUNT_NUMBER=$(echo $ACCOUNT_RESPONSE | grep -o '"accountNumber":"[^"]*' | cut -d'"' -f4)
echo "Created account: $ACCOUNT_NUMBER"

# Test 2: Get Balance
echo -e "\n${GREEN}[Test 2] Getting Balance...${NC}"
curl -s http://localhost:8081/api/v1/accounts/$ACCOUNT_NUMBER/balance | jq '.'

# Test 3: Create second account
echo -e "\n${GREEN}[Test 3] Creating Second Account...${NC}"
ACCOUNT_RESPONSE_2=$(curl -s -X POST http://localhost:8081/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d '{
    "accountHolderName": "Test User 2",
    "email": "test2@example.com",
    "phone": "9000000001",
    "accountType": "SAVINGS",
    "initialDeposit": 100000
  }')

ACCOUNT_NUMBER_2=$(echo $ACCOUNT_RESPONSE_2 | grep -o '"accountNumber":"[^"]*' | cut -d'"' -f4)
echo "Created account: $ACCOUNT_NUMBER_2"

# Test 4: Transfer
echo -e "\n${GREEN}[Test 4] Initiating Transfer...${NC}"
TRANSFER_RESPONSE=$(curl -s -X POST http://localhost:8083/api/v1/transactions/transfer \
  -H "Content-Type: application/json" \
  -d "{
    \"senderAccountNumber\": \"$ACCOUNT_NUMBER\",
    \"receiverAccountNumber\": \"$ACCOUNT_NUMBER_2\",
    \"amount\": 1000
  }")

TX_ID=$(echo $TRANSFER_RESPONSE | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "Transaction ID: $TX_ID"

echo -e "\n${GREEN}[✓] Tests Completed Successfully!${NC}"
```

Run tests:
```bash
chmod +x test-api.sh
./test-api.sh
```

---

## 📈 Monitoring & Debugging

### Check Service Health
```bash
# Account Service
curl http://localhost:8081/actuator/health

# Fraud Detection Service
curl http://localhost:8084/actuator/health

# All services
for port in 8081 8082 8083 8084 8085; do
  echo "Port $port:"
  curl -s http://localhost:$port/actuator/health | jq '.status'
done
```

### View Logs
```bash
# Follow logs for Account Service
tail -f account-service/target/spring.log

# Search for fraud detection
grep "Fraud" fraud-detection-service/target/spring.log

# Search for transaction status
grep "PROCESSING\|COMPLETED\|FLAGGED" transaction-service/target/spring.log
```

### Check Kafka Topics
```bash
# List all topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
bin/kafka-topics.sh --describe --topic transaction.initiated --bootstrap-server localhost:9092

# Consume from topic
bin/kafka-console-consumer.sh --topic transaction.completed --bootstrap-server localhost:9092 --from-beginning

# Check consumer groups
bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
```

### Check Redis
```bash
# Connect to Redis CLI
redis-cli

# Check all keys
KEYS *

# Check OTP for transaction
GET verification:otp:tx_001

# Check fraud velocity
GET fraud:velocity123456789012

# Monitor Redis in real-time
MONITOR

# Clear all data (careful!)
FLUSHALL
```

---

## 📝 Project Structure

```
BankingSystem/
├── account-service/
│   ├── src/main/java/com/banking/accountservice/
│   │   ├── controller/       # REST endpoints
│   │   ├── service/          # Business logic + Kafka listeners
│   │   ├── entity/           # JPA entities
│   │   ├── repository/       # Database access
│   │   └── dto/              # Request/Response DTOs
│   ├── src/main/resources/
│   │   └── application.yaml  # Configuration
│   └── pom.xml              # Maven dependencies
│
├── fraud-detection-service/
│   ├── src/main/java/com/banking/frauddetectionservice/
│   │   ├── service/          # Fraud checking logic
│   │   ├── client/           # OpenFeign Account Service calls
│   │   ├── model/            # Domain models
│   │   └── config/           # Redis, Kafka configs
│   ├── src/main/resources/
│   │   └── application.yaml  # Fraud rules config
│   └── pom.xml
│
├── transaction-service/
│   ├── src/main/java/com/banking/transactionservice/
│   │   ├── service/          # SAGA orchestration
│   │   ├── entity/           # Transaction entity
│   │   ├── controller/       # REST endpoints
│   │   ├── event/            # Event DTOs
│   │   └── client/           # OpenFeign clients
│   └── src/main/resources/
│       └── application.yaml
│
├── payment-service/
│   ├── src/main/java/com/banking/paymentservice/
│   │   ├── service/          # Razorpay integration
│   │   ├── controller/       # Payment endpoints
│   │   └── entity/           # Payment entity
│   └── src/main/resources/
│       └── application.yaml
│
├── notification-service/
│   ├── src/main/java/com/banking/notificationservice/
│   │   └── service/          # Kafka listeners for all events
│   └── src/main/resources/
│       └── application.yaml
│
├── api-gateway/
│   ├── src/main/java/com/banking/apigateway/
│   │   └── config/           # Gateway routes + Redis cache
│   └── src/main/resources/
│       └── application.yaml
│
└── README.md                 # This file
```

---

## 📜 License

This project is licensed only for portfolio and learning.

---


## 🎓 Key Concepts

### Microservices
Independent services handling specific business capabilities, communicating via APIs and events.

### SAGA Pattern
Manages distributed transactions by coordinating a series of local transactions with automatic compensation on failure.

### Event-Driven Architecture
Services communicate asynchronously through events published to Kafka, enabling loose coupling and scalability.

### Fraud Detection
Real-time pattern matching using velocity checks, amount anomalies, and balance verification to identify suspicious transactions.

### Eventual Consistency
System eventually becomes consistent after all events are processed, even if individual operations are asynchronous.

---

## 📚 Resources

- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Apache Kafka Guide](https://kafka.apache.org/documentation/)
- [SAGA Pattern](https://microservices.io/patterns/data/saga.html)
- [Redis Documentation](https://redis.io/documentation)
- [Razorpay Integration](https://razorpay.com/docs/)


