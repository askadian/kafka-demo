"""
server.py  —  Flask API server for the Kafka Pub/Sub demo.

Responsibilities
────────────────
• Serves the single-page UI  (GET /)
• SSE endpoint for live browser updates  (GET /api/stream)
• Triggers order bursts via REST  (POST /api/send)
• Queries the database  (GET /api/orders)
• Runs a Kafka consumer in a background thread that writes to PostgreSQL
  and broadcasts events to every connected SSE client.

Start with:  python server.py
"""

import json
import queue
import random
import threading
import time
from flask import Flask, Response, jsonify, request, send_from_directory
from kafka import KafkaConsumer, KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "orders"
MAX_ORDERS      = 100          # hard cap on burst size
MAX_WINDOW      = 120          # hard cap on window (seconds)
MIN_INTERVAL    = 0.05         # minimum spacing between messages (seconds)

DB_CONFIG = dict(
    host     = "localhost",
    port     = 5432,
    dbname   = "kafka_demo",
    user     = "amkadian",
    password = "Myjob@2017",
)

# ── Flask App ──────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder="ui", static_url_path="")

# ── SSE Broadcast ──────────────────────────────────────────────────────────────
_subscribers: list[queue.Queue] = []
_sub_lock = threading.Lock()


def broadcast(event_type: str, data: dict) -> None:
    """Push a JSON event to every connected SSE client."""
    payload = json.dumps({"type": event_type, "data": data})
    with _sub_lock:
        stale = []
        for q in _subscribers:
            try:
                q.put_nowait(payload)
            except queue.Full:
                stale.append(q)
        for q in stale:
            _subscribers.remove(q)


# ── Sample Data ────────────────────────────────────────────────────────────────
_ITEMS = [
    ("Mechanical Keyboard",          149.99),
    ("Wireless Mouse",                79.99),
    ("4K Monitor",                   599.99),
    ("USB-C Hub",                     49.99),
    ("Laptop Stand",                  39.99),
    ("Webcam 1080p",                  89.99),
    ("Noise-Cancelling Headphones",  299.99),
    ("SSD 1TB",                      129.99),
    ("RAM 32GB",                      89.99),
    ("Gaming Chair",                 449.99),
    ("Desk Lamp",                     35.99),
    ("Cable Organiser",               19.99),
    ("Smart Speaker",                 99.99),
    ("External GPU Dock",            349.99),
    ("Thunderbolt 4 Dock",           199.99),
]

_CUSTOMERS = [
    "Alice", "Bob", "Charlie", "Diana", "Eve",
    "Frank", "Grace", "Henry", "Iris", "Jack",
    "Karen", "Leo", "Maya", "Noah", "Olivia",
]

_seq      = 1000
_seq_lock = threading.Lock()


def _next_order_id() -> str:
    global _seq
    with _seq_lock:
        _seq += 1
        return f"ORD-{_seq}"


def _make_order() -> dict:
    item, unit = random.choice(_ITEMS)
    qty = random.randint(1, 3)
    return {
        "order_id":  _next_order_id(),
        "customer":  random.choice(_CUSTOMERS),
        "item":      item,
        "quantity":  qty,
        "amount":    round(unit * qty, 2),
        "status":    "NEW",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


# ── Kafka Producer ─────────────────────────────────────────────────────────────
_producer      = None
_producer_lock = threading.Lock()


def _get_producer() -> KafkaProducer:
    global _producer
    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode(),
                acks="all",
            )
    return _producer


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("ui", "index.html")


@app.route("/api/stream")
def sse_stream():
    """Server-Sent Events — browsers subscribe here to receive live updates."""

    def _generate():
        q: queue.Queue = queue.Queue(maxsize=300)
        with _sub_lock:
            _subscribers.append(q)
        try:
            # Announce connection
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            while True:
                try:
                    payload = q.get(timeout=20)
                    yield f"data: {payload}\n\n"
                except queue.Empty:
                    yield ": heartbeat\n\n"   # keeps the connection alive
        finally:
            with _sub_lock:
                try:
                    _subscribers.remove(q)
                except ValueError:
                    pass

    return Response(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )


@app.route("/api/send", methods=["POST"])
def send_burst():
    """
    Trigger a burst of orders.
    Body: { "count": <1-100>, "window": <1-120 seconds> }
    """
    body   = request.get_json(force=True) or {}
    count  = max(1, min(int(body.get("count",  1)),  MAX_ORDERS))
    winSec = max(1, min(float(body.get("window", 5)), MAX_WINDOW))

    def _burst():
        interval = winSec / count
        prod = _get_producer()
        for i in range(count):
            order = _make_order()
            prod.send(KAFKA_TOPIC, value=order)
            broadcast("produced", order)
            if i < count - 1 and interval > MIN_INTERVAL:
                time.sleep(interval)
        prod.flush()
        broadcast("burst_done", {"count": count})

    threading.Thread(target=_burst, daemon=True).start()
    return jsonify({"status": "ok", "count": count, "window": winSec})


@app.route("/api/orders")
def get_orders():
    """Return the latest 100 rows from the orders table."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT order_id, customer, item, quantity,
                       amount, status, received_at
                FROM   orders
                ORDER  BY received_at DESC
                LIMIT  100
            """)
            rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        for r in rows:
            r["amount"]      = float(r["amount"])
            r["received_at"] = r["received_at"].isoformat()
        return jsonify(rows)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Kafka Consumer Thread ──────────────────────────────────────────────────────
def _consumer_thread() -> None:
    """
    Runs forever in the background.
    Reads from the 'orders' Kafka topic, persists each message to PostgreSQL,
    then broadcasts a 'consumed' SSE event to all connected browsers.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers   = KAFKA_BOOTSTRAP,
                auto_offset_reset   = "latest",
                group_id            = "order-consumer-demo",
                value_deserializer  = lambda v: json.loads(v.decode()),
                consumer_timeout_ms = -1,    # block indefinitely
            )
            db = psycopg2.connect(**DB_CONFIG)
            print("[consumer] Connected to Kafka + PostgreSQL — listening …")

            for msg in consumer:
                order = msg.value
                try:
                    with db.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO orders
                                (order_id, customer, item, quantity, amount, status)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (order_id) DO NOTHING
                            """,
                            (
                                order["order_id"], order["customer"],
                                order["item"],     order.get("quantity", 1),
                                order["amount"],   order["status"],
                            ),
                        )
                    db.commit()
                    broadcast("consumed", order)
                except Exception as db_err:
                    print(f"[consumer] DB error: {db_err}")
                    db.rollback()
                    # Try to get a fresh connection
                    try:
                        db = psycopg2.connect(**DB_CONFIG)
                    except Exception:
                        pass

        except Exception as exc:
            print(f"[consumer] Error: {exc} — retrying in 5 s …")
            time.sleep(5)


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t = threading.Thread(target=_consumer_thread, daemon=True, name="kafka-consumer")
    t.start()
    print("[server]   Kafka consumer thread started.")
    print("[server]   Open http://localhost:5000 in your browser.")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
