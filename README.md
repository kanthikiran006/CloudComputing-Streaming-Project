<img width="1918" height="965" alt="7" src="https://github.com/user-attachments/assets/eb3e35cd-47bf-4772-aa2f-244337921bbf" /># 🚀 Real-Time Cloud Computing Streaming Analytics Pipeline

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

<img width="1918" height="1017" alt="1" src="https://github.com/user-attachments/assets/0b6386ed-6568-43ae-9b3d-acb5f2598ae6" />
<img width="1918" height="1017" alt="2" src="https://github.com/user-attachments/assets/1c699ff7-5a2f-4372-894f-b1d0a552fd73" />
<img width="1918" height="1017" alt="3" src="https://github.com/user-attachments/assets/746ea368-1f2c-4c77-9f6e-dc970bd53214" />
<img width="1918" height="1020" alt="4" src="https://github.com/user-attachments/assets/48d6e9ce-f516-40a7-9307-5aae414c3697" />
<img width="1918" height="1018" alt="5" src="https://github.com/user-attachments/assets/08699026-d735-4862-9831-3854e54ae09f" />
<img width="1918" height="1018" alt="6" src="https://github.com/user-attachments/assets/3a9195fb-d9d7-4883-8f8f-46a6e1489e15" />
<img width="1918" height="965" alt="7" src="https://github.com/user-attachments/assets/e3753453-a139-4933-b9dc-96070bd0865f" />
<img width="1918" height="913" alt="8" src="https://github.com/user-attachments/assets/be4be298-d8bb-44ac-a2ee-fc84388b91d9" />



---

# 🎥 Project Demo Video

https://drive.google.com/file/d/1aArUNkRx459S5o0PCP_frng7MSkbpvbE/view?usp=drive_link
https://drive.google.com/file/d/1iEVdJXLqbng7oxWSAOlGcXMkp9_t34MA/view?usp=sharing
https://drive.google.com/file/d/1BB2Z610qi_qjQq0WXHO6Jl_OOMeKWg1e/view?usp=drive_link

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

