import serial
import json
from kafka import KafkaProducer
import time

# 🔹 Configure Arduino COM port
ser = serial.Serial('COM6', 9600, timeout=1)

# 🔹 Configure Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 🔥 IMPORTANT FIX
)

print("🚀 Producer Started...")

while True:
    try:
        if ser.in_waiting > 0:
            line = ser.readline().decode().strip()

            # Example input from Arduino:
            # RAW:26.10|AVG:25.92|STATUS:NORMAL|TS:123456

            if "RAW" in line:
                parts = line.split("|")

                raw = float(parts[0].split(":")[1])
                avg = float(parts[1].split(":")[1])
                status = parts[2].split(":")[1]
                timestamp = int(parts[3].split(":")[1])

                data = {
                    "raw": raw,
                    "avg": avg,
                    "status": status,
                    "timestamp": timestamp
                }

                # 🔥 Send to Kafka (JSON format)
                producer.send("sensor-data", value=data)
                producer.flush()

                print("Sent:", data)

        time.sleep(0.1)

    except Exception as e:
        print("Error:", e)