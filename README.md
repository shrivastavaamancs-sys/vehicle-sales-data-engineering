# 🚗 Vehicle Sales Data Engineering Pipeline

## 📌 Project Description

This project implements an end-to-end **Data Engineering pipeline** for processing vehicle sales data using modern Azure cloud technologies.

The pipeline is designed to ingest, store, transform, and process vehicle sales data through different stages using **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, and PySpark**.

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw data into clean, structured, and analytics-ready data.

---

## 🏗️ Architecture

**Source Data → Azure Data Factory → ADLS Gen2 → Azure Databricks / PySpark → Silver Layer → Gold Layer → Analytics**

---

## 🛠️ Technologies Used

- **Azure Data Factory** – Data ingestion and pipeline orchestration
- **Azure Data Lake Storage Gen2** – Cloud data storage
- **Azure Databricks** – Data processing and transformation
- **PySpark** – Distributed data processing
- **Azure Synapse Analytics** – Data warehousing and analytics
- **SQL** – Data querying and transformation
- **Python** – Data processing and automation
- **Apache Airflow** – Workflow orchestration
- **Git & GitHub** – Version control

---

## 🔄 Data Pipeline

### 🥉 Bronze Layer
Raw vehicle sales data is ingested from the source and stored in **Azure Data Lake Storage Gen2** without major transformations.

### 🥈 Silver Layer
The raw data is cleaned and transformed using **Azure Databricks and PySpark**.

Operations include:

- Data cleaning
- Handling missing values
- Data type conversion
- Removing duplicate records
- Data validation
- Standardizing data

### 🥇 Gold Layer
The cleaned data is transformed into business-ready datasets for reporting and analytics.

This layer contains structured and aggregated data that can be consumed by analytical tools such as **Power BI and Azure Synapse Analytics**.

---

## 🎯 Key Features

- End-to-end Azure Data Engineering pipeline
- Automated data ingestion using Azure Data Factory
- Scalable cloud storage using ADLS Gen2
- Distributed data processing using PySpark
- Data transformation using Azure Databricks
- Medallion Architecture implementation
- Bronze, Silver, and Gold data layers
- Data cleaning and validation
- ETL/ELT pipeline implementation
- Analytics-ready datasets
- Version control using Git and GitHub

---

## 📂 Repository Structure

```text
Vehicle-Sales-Data-Engineering-Pipeline/
│
├── airflow/
│   └── Airflow DAGs and workflow configuration
│
├── data/
│   └── Source and processed data
│
├── dataset/
│   └── Dataset configuration and files
│
├── factory/
│   └── Azure Data Factory configuration
│
├── linkedService/
│   └── Azure Data Factory linked services
│
├── pipeline/
│   └── Azure Data Factory pipelines
│
├── src/
│   └── PySpark / Python transformation scripts
│
├── .gitignore
│
└── publish_config.json*****
