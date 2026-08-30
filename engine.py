import json
import time
import os
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'redpanda:9092')
TOPIC_NAME = 'ingestion-stream'
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_USER = os.getenv('POSTGRES_USER', 'admin')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'secretpassword')
PG_DB = os.getenv('POSTGRES_DB', 'execution_engine')

def get_db_connection():
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

def compute_wave_collapse(p_valid, v_anchor, p_corrupt, v_crucible, omega_system=1.5):
    """Calculates Base_EV and Collapse(Psi)"""
    base_ev = (p_valid * v_anchor) - (p_corrupt * v_crucible)
    collapse_psi = base_ev * omega_system
    return collapse_psi

def main():
    conn = get_db_connection()
    init_db(conn)

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        max_poll_records=500, # Pull up to 500 records at a time
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Quantum EV Logic Engine listening for streams...")

    while True:
        # Micro-batch polling instead of a blocking 1-by-1 loop
        records = consumer.poll(timeout_ms=100)
        if not records:
            continue

        insert_batch = []
        for tp, messages in records.items():
            for message in messages:
                data = message.value
                
                # Scoring dynamic utility
                ev_score = compute_wave_collapse(
                    p_valid=0.95,
                    v_anchor=data.get('ev_value', 1.0),
                    p_corrupt=0.05,
                    v_crucible=0.1,
                    omega_system=1.5
                )

                if ev_score > 0.0:
                    insert_batch.append((
                        data.get('id'),
                        data.get('source'),
                        ev_score,
                        data.get('payload')
                    ))
                else:
                    print(f"[ANNIHILATED] ID: {data.get('id')} | Dropped")

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
    time.sleep(5)  # Wait for broker startup
    main()
