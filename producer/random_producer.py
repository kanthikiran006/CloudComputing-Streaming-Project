from kafka import KafkaProducer
import json
import time
import random
import uuid

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topics = ["logs", "transactions", "reviews", "social-media"]

locations = ["India", "USA", "UK", "Germany"]
devices = ["mobile", "desktop"]
pages = ["home", "product", "cart", "checkout"]

def generate_log():
    return {
        "type": "log",
        "session_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "page": random.choice(pages),
        "device": random.choice(devices),
        "location": random.choice(locations),
        "response_time": round(random.uniform(0.1, 2.5), 2),
        "status_code": random.choice([200, 200, 200, 404, 500]),
        "timestamp": int(time.time())
    }

def generate_transaction():
    amount = round(random.uniform(10, 5000), 2)
    user_location = random.choice(locations)
    transaction_location = random.choice(locations)

    fraud = False
    if amount > 3000 or user_location != transaction_location:
        fraud = True

    return {
        "type": "transaction",
        "user_id": random.randint(1, 1000),
        "amount": amount,
        "payment_method": random.choice(["card", "upi", "netbanking"]),
        "user_location": user_location,
        "transaction_location": transaction_location,
        "fraud": fraud,
        "timestamp": int(time.time())
    }

def generate_review():
    rating = random.randint(1, 5)
    sentiment = "positive" if rating >= 4 else "negative" if rating <= 2 else "neutral"

    return {
        "type": "review",
        "user_id": random.randint(1, 1000),
        "rating": rating,
        "sentiment": sentiment,
        "review_text": random.choice([
            "Excellent product", "Not satisfied", "Worth the money", "Very bad experience"
        ]),
        "timestamp": int(time.time())
    }

def generate_social():
    base = random.randint(10, 100)

    # simulate trending spike
    if random.random() < 0.1:
        base *= 10

    return {
        "type": "social",
        "user_id": random.randint(1, 1000),
        "likes": base,
        "shares": random.randint(0, base // 2),
        "comments": random.randint(0, base // 3),
        "timestamp": int(time.time())
    }

while True:
    topic = random.choice(topics)

    if topic == "logs":
        data = generate_log()
    elif topic == "transactions":
        data = generate_transaction()
    elif topic == "reviews":
        data = generate_review()
    else:
        data = generate_social()

    producer.send(topic, value=data)
    print(f"Sent to {topic}: {data}")

    time.sleep(1)
