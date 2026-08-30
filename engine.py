import json
import time
import os
import signal
import sys
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from prometheus_client import start_http_server, Counter, Gauge, Histogram

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'redpanda:9092')
TOPIC_NAME = 'ingestion-stream'
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_USER = os.getenv('POSTGRES_USER', 'admin')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secretpassword')
PG_DB = os.getenv('POSTGRES_DB', 'execution_engine')

MESSAGES_EVALUATED = Counter('tiee_messages_evaluated_total', 'Total incoming payloads evaluated')
STATES_COLLAPSED = Counter('tiee_states_collapsed_total', 'Total states constructively collapsed')
STATES_ANNIHILATED = Counter('tiee_states_annihilated_total', 'Total noise states mathematically annihilated')
EV_SCORE_DISTRIBUTION = Histogram('tiee_ev_score_distribution', 'Distribution of EV scores')
BATCH_SIZE_GAUGE = Gauge('tiee_current_batch_size', 'Current micro-batch size')

shutdown_flag = False

def sigterm_handler(signum, frame):
    global shutdown_flag
    print("\n[SYSTEM] SIGTERM received. Gracefully shutting down and flushing batches...")
    shutdown_flag = True

signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

def get_db_connection():
    while not shutdown_flag:
        try:
            return psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB)
        except psycopg2.OperationalError:
            print("Waiting for King's Chamber DB...")
            time.sleep(2)

def init_db(conn):
    if not conn: return
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
    while not shutdown_flag:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=500,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except Exception as e:
            print(f"Waiting for Redpanda... ({e})")
            time.sleep(2)

def compute_wave_collapse(p_valid, v_anchor, p_corrupt, v_crucible, omega_system=1.5):
    return ((p_valid * v_anchor) - (p_corrupt * v_crucible)) * omega_system

def main():
    start_http_server(8000)
    conn = get_db_connection()
    init_db(conn)
    consumer = get_kafka_consumer()

    print("Quantum EV Logic Engine listening for streams...")

    while not shutdown_flag:
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
                ev_score = compute_wave_collapse(0.95, data.get('ev_value', 1.0), 0.05, 0.1, 1.5)
                EV_SCORE_DISTRIBUTION.observe(ev_score)

                if ev_score > 0.0:
                    STATES_COLLAPSED.inc()
                    insert_batch.append((data.get('id'), data.get('source'), ev_score, data.get('payload')))
                else:
                    STATES_ANNIHILATED.inc()

        BATCH_SIZE_GAUGE.set(batch_count)

        if insert_batch and conn:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO collapsed_records (id, source, score, payload) VALUES %s ON CONFLICT DO NOTHING", insert_batch)
                conn.commit()

    if consumer: consumer.close()
    if conn: conn.close()
    print("[SYSTEM] Shutdown complete.")

if __name__ == '__main__':
    main()
