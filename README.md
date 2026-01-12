# Real-Time Product Recommendation System (Databricks – Online ML)

## 📌 Project Overview
This project implements an **end-to-end real-time product recommendation system** using **Databricks Lakehouse architecture**. It demonstrates how to build **incremental data pipelines**, apply **Delta Live Tables (DLT)**, and develop a **production-grade online machine learning workflow**.

The solution covers the complete lifecycle:  
**Data generation → Medallion (Bronze–Silver–Gold) pipeline → Feature engineering → Candidate generation → Model training → Inference & recommendation snapshots**

---

## 🏗️ High-Level Architecture

Faker / Source Data
↓
Bronze Layer (DLT – Raw Ingestion)
↓
Silver Layer (DLT – Cleaned & Enriched)
↓
Gold Layer (DLT – ML-Ready Tables)
↓
Candidate Generation
↓
Feature Engineering
↓
Model Training (XGBoost Ranker)
↓
Inference & Recommendation Snapshots
↓
Databricks SQL Dashboards
Real-time-product-recommendation-ml-databricks/
│
├── Product_Recommendation_Online_ML/
│ ├── 1_Data_Generation.py
│ ├── 3_EDA.py
│ ├── 4_Candidate_Generation.py
│ ├── 5_Feature_Eng.py
│ ├── 6_Label_generation.py
│ ├── 7_Data_splitting.py
│ ├── 8_Model_Training.py
│ └── 9_inference_recommendation.py
│
├── product_recommendation_dlt_Medallion/
│ └── transformations/
│ ├── bronze.sql
│ ├── silver.sql
│ └── gold.sql
│
├── dashboards/
│ └── product_recommendation_dashboard.json
│
└── README.md


---


## 🧱 Data Pipeline – Medallion Architecture (DLT)

### 🔹 Bronze Layer
- Raw data ingestion using Delta Live Tables
- Handles schema evolution
- Append-only ingestion

### 🔹 Silver Layer
- Data cleansing and normalization
- Deduplication and enrichment
- Business-level transformations

### 🔹 Gold Layer
- Feature-ready tables for machine learning
- Aggregated user–product interactions
- Time-aware and leakage-safe joins

---

## 🤖 Machine Learning Pipeline

### Candidate Generation
- Generates relevant **user–product pairs**
- Reduces the search space for ranking

### Feature Engineering
- User interaction signals (views, carts, purchases)
- Product attributes (ratings, reviews, discounts)
- Recency and frequency-based features

### Label Generation
- Creates supervised labels from historical interactions
- Enables ranking-based learning

### Model Training
- **Algorithm**: XGBoost Ranking Model
- Handles non-linear feature interactions
- Built-in regularization to prevent overfitting
- Experiment tracking using **MLflow**

---

## 📊 Inference & Recommendations

- Generates **Top-K product recommendations per user**
- Supports incremental inference
- Handles cold-start users
- Stores output as recommendation snapshots

---

## 📈 Dashboards & Monitoring

This project includes **Databricks SQL Dashboards** for monitoring and analysis.

### Dashboard Capabilities
- Recommendation coverage per user
- Top recommended products
- Model performance metrics
- User interaction trends
- Incremental data freshness

### Version Control
- Dashboards are **exported as JSON**
- Stored in the `dashboards/` folder
- Version-controlled using **Git**
- Can be re-imported into Databricks and refreshed using live data

> Dashboards remain fully interactive after import and always query the latest Bronze/Silver/Gold tables.

---

## 🛠️ Technology Stack

- Databricks Lakehouse
- Apache Spark (PySpark & SQL)
- Delta Lake & Delta Live Tables (DLT)
- MLflow (Experiments & Model Registry)
- XGBoost
- Databricks SQL Dashboards
- GitHub

---

## 🚀 How to Run the Project

1. Clone the repository into **Databricks Repos**
2. Run `1_Data_Generation.py` to generate source data
3. Deploy DLT pipeline:
   - `bronze.sql`
   - `silver.sql`
   - `gold.sql`
4. Execute ML scripts in sequence:
   - Candidate generation
   - Feature engineering
   - Label generation
   - Data splitting
   - Model training
5. Run inference script to generate recommendations
6. Import dashboard JSON and refresh to visualize results

---

## ⭐ Key Highlights

- End-to-end **online ML recommendation system**
- Incremental and scalable DLT pipelines
- Leakage-free model training
- Cold-start user handling
- Dashboard-driven monitoring
- Production-ready Databricks implementation

---

## 🔮 Future Enhancements

- Real-time model serving
- Online feature store integration
- Automated dashboard refresh
- A/B testing for recommendation quality
- Deep learning–based recommenders

---

## 👩‍💻 Author

**Sinchana R**  
Data Engineering | Machine Learning | Databricks

---

⭐ If you find this project useful, consider giving it a star!
