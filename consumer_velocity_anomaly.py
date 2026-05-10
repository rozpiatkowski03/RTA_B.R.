from kafka import KafkaConsumer
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json

WINDOW_SECONDS = 60
MAX_TRANSACTIONS = 3

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='velocity-anomaly-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Stan konsumenta:
# dla każdego user_id przechowujemy timestampy jego ostatnich transakcji
user_transactions = defaultdict(deque)

def parse_timestamp(timestamp_value):
    try:
        return datetime.fromisoformat(timestamp_value)
    except Exception:
        return datetime.now()

print("Konsument wykrywający anomalie prędkości")
print("Alert: więcej niż 3 transakcje tego samego user_id w ciągu 60 sekund\n")

for message in consumer:
    tx = message.value

    user_id = tx.get('user_id')
    tx_id = tx.get('tx_id')
    amount = tx.get('amount')
    store = tx.get('store')
    timestamp = parse_timestamp(tx.get('timestamp'))

    if user_id is None:
        continue

    window_start = timestamp - timedelta(seconds=WINDOW_SECONDS)

    events = user_transactions[user_id]
    events.append(timestamp)

    # Usuwamy transakcje starsze niż 60 sekund względem aktualnej transakcji
    while events and events[0] < window_start:
        events.popleft()

    print(
        f"TX: {tx_id} | user_id={user_id} | "
        f"amount={amount} | store={store} | "
        f"liczba_tx_w_60s={len(events)}"
    )

    if len(events) > MAX_TRANSACTIONS:
        print(
            "\nALERT VELOCITY ANOMALY: "
            f"user_id={user_id} wykonał {len(events)} transakcje/transakcji "
            f"w ciągu {WINDOW_SECONDS} sekund | "
            f"ostatnia transakcja={tx_id} | "
            f"amount={amount} | store={store}\n"
        )
