from kafka import KafkaProducer
from datetime import datetime
import json
import time

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(4):
    tx = {
        'tx_id': f'TEST{i+1:04d}',
        'user_id': 'u99',
        'amount': 100 + i,
        'store': 'Warszawa',
        'category': 'test',
        'timestamp': datetime.now().isoformat()
    }

    producer.send('transactions', value=tx)
    print(f"Wysłano: {tx}")
    time.sleep(1)

producer.flush()
producer.close()
print("Gotowe.")
