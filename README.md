# kafka-demo
Simple application to show Apache Kafka implementation
# Kafka Pub/Sub Demo — Project Plan

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LOCAL MACHINE                            │
│                                                                 │
│  ┌──────────┐    ┌─────────────────────┐    ┌──────────────┐  │
│  │ Producer │───▶│       KAFKA         │───▶│   Consumer   │  │
│  │ (Python) │    │  (Docker Container) │    │   (Python)   │  │
│  └──────────┘    └─────────────────────┘    └──────┬───────┘  │
│       ▲                                            │           │
│       │                                     ┌──────▼───────┐  │
│       │                                     │  PostgreSQL  │  │
│       │                                     │  (local DB)  │  │
│       │                                     └──────┬───────┘  │
│       │                                            │           │
│  ┌────┴────────────────────────────────────────────┴───────┐   │
│  │              Flask API Server (Python)                  │   │
│  │         REST + Server-Sent Events (SSE)                 │   │
│  └────────────────────────┬────────────────────────────────┘   │
│                           │                                     │
│                    ┌──────▼──────┐                              │
│                    │  Browser UI │                              │
│                    │ (HTML/JS)   │                              │
│                    └─────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology | Why |
|---|---|---|
| Message Broker | **Apache Kafka** via Docker | Industry standard, easy local setup |
| Kafka Coordinator | **ZooKeeper** via Docker | Required by Kafka |
| Producer | **Python** (`kafka-python`) | Simple, readable |
| Consumer | **Python** (`kafka-python`) | Same ecosystem |
| API Server | **Flask** + SSE | Lightweight, real-time push |
| Database | **PostgreSQL** (local install) | Your existing local DB |
| UI | **HTML + Vanilla JS** | No build tools, just open in browser |
| Orchestration | **Docker Compose** | Kafka + ZooKeeper in one command |

---

## Sample Application — "Order Processing System"

A simple e-commerce order flow. Realistic and easy to understand.

```
Customer places order
       ↓
  [Producer] publishes order event to Kafka topic: "orders"
       ↓
  [Kafka] holds and delivers the message
       ↓
  [Consumer] reads the message, writes to PostgreSQL
       ↓
  [Flask API] queries DB, pushes live updates to browser via SSE
       ↓
  [UI] shows animated message flow + message log table
```

### Sample Message (JSON)
```json
{
  "order_id": "ORD-1042",
  "customer": "Alice",
  "item": "Mechanical Keyboard",
  "amount": 149.99,
  "status": "NEW",
  "timestamp": "2026-03-27T14:32:01Z"
}
```

### PostgreSQL Table: `orders`
| Column | Type |
|---|---|
| order_id | VARCHAR (PK) |
| customer | VARCHAR |
| item | VARCHAR |
| amount | DECIMAL |
| status | VARCHAR |
| received_at | TIMESTAMP |

---

## UI Design Concept

Three panels on one page:

```
┌─────────────────────────────────────────────────────────┐
│  KAFKA PUB/SUB DEMO — Order Processing                  │
├──────────────┬──────────────────┬───────────────────────┤
│  PRODUCER    │     KAFKA        │   CONSUMER + DB       │
│              │                  │                       │
│  [Send Order]│  ◉ orders topic  │  ✓ ORD-1042 saved    │
│              │  → → → → → → → →│  ✓ ORD-1041 saved    │
│  Auto-send   │                  │                       │
│  [Toggle ON] │  Messages in     │  [View DB Table ↓]   │
│              │  flight: 3       │                       │
├──────────────┴──────────────────┴───────────────────────┤
│  LIVE MESSAGE LOG                                       │
│  ORD-1042 | Alice | Keyboard | $149.99 | 14:32:01      │
│  ORD-1041 | Bob   | Monitor  | $299.00 | 14:31:55      │
└─────────────────────────────────────────────────────────┘
```

---

## Project File Structure

```
kafka-demo/
├── docker-compose.yml        # Kafka + ZooKeeper
├── requirements.txt          # Python dependencies
├── producer.py               # Publishes orders to Kafka
├── consumer.py               # Reads Kafka, writes to PostgreSQL
├── server.py                 # Flask API + SSE endpoint
├── init_db.py                # Creates PostgreSQL table
└── ui/
    └── index.html            # Single-page UI
```

---

## Startup Sequence

```
Step 1: docker-compose up          → starts Kafka + ZooKeeper
Step 2: python init_db.py          → creates orders table in PostgreSQL
Step 3: python consumer.py         → starts listening to Kafka topic
Step 4: python server.py           → starts Flask API + serves UI
Step 5: Open browser → localhost:5000
Step 6: Click "Send Order" or enable auto-send
```

---

All good. Here's your environment status:

| Component | Status |
|---|---|
| Docker | v29.3.1 |
| Docker Compose | v5.1.0 |
| Python | 3.14.0 |
| pip | 25.3 |
| PostgreSQL | 17.5 (Postgres.app), running on localhost:5432 |

---

## Updated Plan — Burst Send Feature

The UI will have two sliders/inputs before the Send button:

```
┌──────────────────────────────────────────────────────────────┐
│  SEND ORDERS                                                 │
│                                                              │
│  Number of Orders:  [====●────]  25   (max: 100)            │
│  Over Time Window:  [==●──────]  30s  (max: 120s)           │
│                                                              │
│          [ SEND BURST ]                                      │
│                                                              │
│  → Orders will be spaced ~1.2s apart                        │
└──────────────────────────────────────────────────────────────┘
```

**How it works:**
- User sets N orders (1–100) and window W seconds (1–120)
- Clicking "Send Burst" calls the Flask API: `POST /send?count=25&window=30`
- Flask spawns a background thread that sends orders to Kafka spaced `W/N` seconds apart
- Each order appears in the UI live as it flows through Kafka → Consumer → DB

**Constraints:**
- Max orders: **100**
- Max window: **120 seconds**
- Minimum interval between messages: **0.1s** (to avoid overwhelming local Kafka)
- The interval label updates live as sliders change (`~X.Xs apart`)

---

# 1. Start Kafka
cd ~/Desktop/kafka-demo && docker compose up -d

# 2. Start the server
source venv/bin/activate && python server.py

# 3. Open browser
open http://localhost:5000
