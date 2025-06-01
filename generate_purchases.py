from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import uuid

# Настройка Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Тестовые данные
product_ids = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"]
customer_ids = ["c1", "c2", "c3", "c4", "c5"]

def generate_purchase():
    """Генерирует одно событие покупки"""
    product_id = random.choice(product_ids)
    quantity = random.randint(1, 5)
    
    # Цена зависит от product_id
    price_map = {
        "p1": 1200.00, "p2": 800.00, "p3": 150.00, "p4": 25.00,
        "p5": 60.00, "p6": 15.00, "p7": 10.00, "p8": 3.00
    }
    price = price_map.get(product_id, 10.00)
    
    # Случайное время в пределах последнего часа
    now = datetime.now()
    random_minutes = random.randint(0, 59)
    purchase_time = (now - timedelta(minutes=random_minutes)).isoformat()
    
    return {
        "purchase_id": str(uuid.uuid4()),
        "customer_id": random.choice(customer_ids),
        "product_id": product_id,
        "quantity": quantity,
        "amount": round(price * quantity, 2),
        "purchase_time": purchase_time
    }

def send_purchases(num_events=100, interval=0.5):
    """Отправляет указанное количество событий покупок в Kafka"""
    for i in range(num_events):
        purchase = generate_purchase()
        producer.send('purchases', purchase)
        print(f"Sent purchase: {purchase}")
        time.sleep(interval)
    
    producer.flush()
    print(f"Sent {num_events} purchase events")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate purchase events for Kafka')
    parser.add_argument('--events', type=int, default=100, help='Number of events to generate')
    parser.add_argument('--interval', type=float, default=0.5, help='Interval between events in seconds')
    
    args = parser.parse_args()
    
    print(f"Generating {args.events} purchase events with interval {args.interval} seconds...")
    send_purchases(args.events, args.interval)