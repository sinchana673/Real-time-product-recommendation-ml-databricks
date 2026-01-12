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

## 📁 Repository Structure
Real-time-product-recommendation-ml-databricks/
│
├── Product_Recommendation_Online_ML/
│ ├── 1_Data_Generation
│ ├── 3_EDA
│ ├── 4_Candidate_Generation
│ ├── 5_Feature_Eng
│ ├── 6_Label_generation
│ ├── 7_Data_splitting
│ ├── 8_Model_Training
│ └── 9_inference_recommendation
│
├── product_recommendation_dlt_Medallion/
│ └── transformations/
│ ├── bronze.sql
│ ├── silver.sql
│ └── gold.sql
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

## 🛠️ Technology Stack

- Databricks Lakehouse
- Apache Spark (PySpark & SQL)
- Delta Lake & Delta Live Tables (DLT)
- MLflow (Experiments & Model Registry)
- XGBoost
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

---

## ⭐ Key Highlights

- End-to-end **online ML recommendation system**
- Incremental and scalable DLT pipelines
- Leakage-free model training
- Cold-start user handling
- Production-ready Databricks implementation

---

## 🔮 Future Enhancements

- Real-time model serving
- Online feature store integration
- A/B testing for recommendation quality
- Deep learning–based recommenders

---

## 👩‍💻 Author

**Sinchana R**  
Data Engineering | Machine Learning | Databricks

---

⭐ If you find this project useful, consider giving it a star!
