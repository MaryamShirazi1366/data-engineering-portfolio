# Reddit Data Pipeline Project 
# Reddit Data Pipeline 🚀

## 📌 Overview
This project provides a **comprehensive data pipeline** to extract, transform, and load (**ETL**) Reddit data into an **Amazon Redshift** data warehouse. It leverages **Apache Airflow, Celery, PostgreSQL, Amazon S3, AWS Glue, Amazon Athena, and Redshift** to create a fully automated **big data pipeline**.

## ✅ Features
- **🔄 Automated Workflow** → Uses **Airflow & Celery** to schedule and distribute ETL tasks.
- **📥 Data Extraction** → Pulls data from **Reddit API**.
- **💾 Temporary Storage** → Saves raw data in **PostgreSQL** before transformation.
- **📂 Cloud Storage** → Moves raw JSON data to **Amazon S3**.
- **🛠 Data Transformation** → Uses **AWS Glue & Athena** for cleaning and structuring data.
- **📊 Data Warehouse** → Loads structured data into **Amazon Redshift** for analytics.
- **🛡️ Scalable & Distributed** → Dockerized setup allows **horizontal scaling** with **Celery workers**.

---

## 📂 Architecture  
🚀 **High-Level Flow:**
### **🔹 Architecture Components**
| **Component**     | **Description** |
|-------------------|----------------|
| **Reddit API** | Source of raw Reddit data |
| **Apache Airflow + Celery** | Orchestrates and schedules the ETL pipeline |
| **PostgreSQL** | Temporary staging database for initial data storage |
| **Amazon S3** | Stores raw extracted data in JSON format |
| **AWS Glue** | ETL job that processes and catalogs data |
| **Amazon Athena** | SQL-based transformation of raw data |
| **Amazon Redshift** | Final data warehouse for analytics |

---

## 🎯 Prerequisites  
Before setting up the pipeline, ensure you have:
- ✅ **AWS Account** (Permissions for **S3, Glue, Athena, Redshift**)
- ✅ **Reddit API Credentials** (Client ID, Secret)
- ✅ **Docker Installed**
- ✅ **Python 3.9 or higher**
- ✅ **PostgreSQL Installed** (or running via Docker)

---

📊 Expected Output
📁 Raw Reddit JSON → Stored in Amazon S3 (s3://your-bucket/reddit/raw/)
🛠 Processed Data → Available in Amazon Athena (aws-glue-table/reddit_data)
📊 Final Analytics Data → Stored in Amazon Redshift
🎯 Future Enhancements
✅ Enable Streaming with Kafka
✅ Integrate with Snowflake
✅ Enhance Real-time Analytics



