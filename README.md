# 🚀 Real-Time Cloud Computing Streaming Analytics Pipeline

## 📌 Project Overview
This project is a real-time Big Data Streaming and Analytics Pipeline developed using Apache Kafka, Apache Spark Structured Streaming, Python, and Streamlit Dashboard.

The system simulates real-world continuous data streams such as:
- Transactions
- Logs
- Reviews
- Social Media Data

The incoming streaming data is processed in real-time using Spark Structured Streaming, stored in Parquet format, and visualized through an interactive Streamlit dashboard.

---

# 🏗️ Architecture

Producer → Kafka → Spark Analytics → Parquet Storage → Streamlit Dashboard

---

# ⚙️ Technologies Used

- Apache Kafka
- Apache Spark Structured Streaming
- Python
- Streamlit
- Parquet Storage
- WSL Ubuntu

---

# 🔥 Features

✅ Real-time data streaming  
✅ Multi-topic Kafka architecture  
✅ Fraud detection analytics  
✅ Revenue analytics  
✅ User engagement analytics  
✅ Sentiment analysis  
✅ Window-based aggregation  
✅ Watermarking  
✅ Live dashboard visualization  

---

# 📊 Dashboard Output

<img width="1918" height="1018" alt="6" src="https://github.com/user-attachments/assets/6ec33f21-e80f-4de6-bdd3-7fe9d6ff20bd" />


---

# 🎥 Project Demo Video

(Add Google Drive or YouTube video link here)

---

# 🚀 How To Run

## Start Zookeeper
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

## Start Kafka
```bash
bin/kafka-server-start.sh config/server.properties
```

## Run Producer
```bash
python3 random_producer.py
```

## Run Spark Analytics
```bash
spark-submit spark_multi_analytics.py
```

## Run Dashboard
```bash
streamlit run dashboard.py
```

---

# 👨‍💻 Developed By

Kanthi Kiran (RA2512051010004)
RAVI BHARATHI K (RA2512052010029)
Sri Harsha Pattnayak (RA2512052010023)
Venkat Srinivas (RA2512052010024)
M BHANUPRAKASH (RA2512052010019)

