import json
import time
from kafka import KafkaConsumer

KAFKA_BROKER = 'redpanda:9092'
TOPIC_NAME = 'ingestion-stream'

def compute_wave_collapse(p_valid, v_anchor, p_corrupt, v_crucible, omega_system=1.5):
    """
    Calculates Base_EV and Collapse(Psi)
    """
    base_ev = (p_valid * v_anchor) - (p_corrupt * v_crucible)
    collapse_psi = base_ev * omega_system
    return collapse_psi

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Quantum EV Logic Engine listening for streams...")
    for message in consumer:
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
            print(f"[COLLAPSE SUCCESS] ID: {data.get('id')} | Score: {ev_score:.4f} -> Routed to Storage")
        else:
            print(f"[ANNIHILATED] ID: {data.get('id')} | Low EV Score -> Dropped")

if __name__ == '__main__':
    time.sleep(5)  # Wait for broker startup
    main()

