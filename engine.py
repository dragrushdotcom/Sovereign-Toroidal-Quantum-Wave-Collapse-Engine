import json
import time
import os
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# --- Environment Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'redpanda:9092')
TOPIC_NAME = 'ingestion-stream'
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_USER = os.getenv('POSTGRES_USER', 'admin')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secretpassword')
PG_DB = os.getenv('POSTGRES_DB', 'execution_engine')

# --- Prometheus Metrics Definitions ---
MESSAGES_EVALUATED = Counter('tiee_messages_evaluated_total', 'Total incoming payloads evaluated by the logic engine')
STATES_COLLAPSED = Counter('tiee_states_collapsed_total', 'Total states constructively collapsed and routed to storage')
STATES_ANNIHILATED = Counter('tiee_states_annihilated_total', 'Total noise states mathematically annihilated (dropped)')
EV_SCORE_DISTRIBUTION = Histogram('tiee_ev_score_distribution', 'Distribution of calculated wave-collapse EV scores')
BATCH_SIZE_GAUGE = Gauge('tiee_current_batch_size', 'Number of records processed in the current micro-batch')

def get_db_connection():
    """Resilient connection loop for King's Chamber DB."""
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DB
            )
            return conn
        except psycopg2.OperationalError:
            print("Waiting for King's Chamber DB to become ready...")
            time.sleep(2)

def init_db(conn):
    """Initializes the persistent ledger schema."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS collapsed_records (
                id VARCHAR(128) PRIMARY KEY,
                source VARCHAR(128),
                score FLOAT,
                payload TEXT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def get_kafka_consumer():
    """Resilient connection loop for the Grand Gallery broker."""
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=500, # Pull up to 500 records at a time
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("Successfully connected to Grand Gallery Broker.")
            return consumer
        except Exception as e:
            print(f"Waiting for Grand Gallery (Redpanda)... ({e})")
            time.sleep(2)

def compute_wave_collapse(p_valid, v_anchor, p_corrupt, v_crucible, omega_system=1.5):
    """Calculates Base_EV and Collapse(Psi)"""
    base_ev = (p_valid * v_anchor) - (p_corrupt * v_crucible)
    collapse_psi = base_ev * omega_system
    return collapse_psi

def main():
    # Start the Prometheus metrics server on port 8000
    start_http_server(8000)
    print("Prometheus metrics server started on port 8000.")

    conn = get_db_connection()
    init_db(conn)
    consumer = get_kafka_consumer()

    print("Quantum EV Logic Engine listening for streams...")

    while True:
        # Micro-batch polling instead of a blocking 1-by-1 loop
        records = consumer.poll(timeout_ms=100)
        if not records:
            continue

        insert_batch = []
        batch_count = 0
        
        for tp, messages in records.items():
            batch_count += len(messages)
            for message in messages:
                MESSAGES_EVALUATED.inc()
                data = message.value
                
                # Scoring dynamic utility
                ev_score = compute_wave_collapse(
                    p_valid=0.95,
                    v_anchor=data.get('ev_value', 1.0),
                    p_corrupt=0.05,
                    v_crucible=0.1,
                    omega_system=1.5
                )
                
                # Expose score to Prometheus
                EV_SCORE_DISTRIBUTION.observe(ev_score)

                if ev_score > 0.0:
                    STATES_COLLAPSED.inc()
                    insert_batch.append((
                        data.get('id'),
                        data.get('source'),
                        ev_score,
                        data.get('payload')
                    ))
                else:
                    STATES_ANNIHILATED.inc()
                    # Removed standard print statement to prevent I/O blocking at high throughput

        # Update batch gauge
        BATCH_SIZE_GAUGE.set(batch_count)

        # Bulk insert to PostgreSQL
        if insert_batch:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO collapsed_records (id, source, score, payload) VALUES %s ON CONFLICT (id) DO NOTHING",
                    insert_batch
                )
                conn.commit()
            print(f"[COLLAPSE SUCCESS] Processed & stored {len(insert_batch)} states.")

if __name__ == '__main__':
    time.sleep(5)  # Wait for broker startup routing
    main()
