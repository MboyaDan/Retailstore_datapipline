# 🏪 Retail Store Data Pipeline

## 📌 Project Overview
This project builds an **end-to-end Data Pipeline** for a **Retail Store’s Sales Analytics** using **Apache Airflow**. The pipeline automates the **Extract, Transform, and Load (ETL)** process, moving data from **PostgreSQL to Google BigQuery** for analysis.

## 🚀 Workflow
### **1️⃣ Data Extraction (PostgreSQL → Airflow)**
- **Source:** PostgreSQL Database (`retail_store`)
- **Process:** Airflow pulls data using **Postgres Operator**
- **Frequency:** Daily (Scheduled in Airflow)

### **2️⃣ Data Transformation (Apache Spark)**
- **Process:** Clean and aggregate data for analytics
- **Technology:** Apache Spark

### **3️⃣ Data Loading (BigQuery)**
- **Process:** Load transformed data into **Google BigQuery**
- **Technology:** BigQuery (GCP)

### **4️⃣ Data Analysis (BigQuery & Visualization)**
- **Use:** Analyze sales trends, revenue, and customer behavior
- **Tools:** SQL Queries on BigQuery, Google Data Studio (Optional)

## 🛠 Tech Stack
| **Component**  | **Technology** |
|--------------|--------------|
| **Workflow Orchestration** | Apache Airflow (inside Docker) |
| **Database** | PostgreSQL (retail_store DB) |
| **Data Extraction** | Airflow Postgres Operator |
| **Data Transformation** | Apache Spark |
| **Storage & Analytics** | Google BigQuery |
| **Containerization** | Docker + Docker Compose |
| **Version Control** | GitHub |
| **Cloud** | Google Cloud Platform (GCP) |

## 📂 Folder Structure
```
RS_Data_Pipline/
│── dags/                     # Airflow DAGs folder
│   ├── retail_dag.py         # Main DAG file
│── scripts/                  # SQL & Python scripts
│── data/                     # Sample dataset (if any, usually ignored in Git)
│── docker-compose.yml        # Docker setup for Airflow & PostgreSQL
│── Dockerfile                # Docker image definition for Airflow environment
│── .gitignore                # Git ignore file
│── README.md                 # Project documentation

```

## 📌 Installation & Setup
1. **Clone the Repository**
   ```sh
   git clone https://github.com/MboyaDan/Retailstore_datapipline.git
   cd RS_Data_Pipline
   ```

2. **Set Up Environment Variables**
   ```sh
   export POSTGRES_USER=mboya
   export POSTGRES_PASSWORD=root
   export POSTGRES_DB=retail_store
   export AIRFLOW_ADMIN_USER=admin
   export AIRFLOW_ADMIN_PASSWORD=admin
   ```

3. **Start Docker Containers**
   ```sh
   docker-compose up -d
   ```

4. **Access Airflow UI**
   - Open your browser and go to: `http://localhost:8080`
   - Login using:
     - **Username:** admin
     - **Password:** admin

5. **Verify DAGs in Airflow**
   ```sh
   docker exec -it airflow_webserver airflow dags list
   ```

## 📌 Next Steps
✅ Define PostgreSQL **schema** and **insert sample data**
✅ Develop **Airflow DAGs** for ETL process
✅ Implement **Spark transformations**
✅ Load data into **BigQuery**
✅ Perform **analysis & visualization**

## 🤝 Contributing
Feel free to open issues or submit pull requests for improvements!

## 📜 License
MIT License. See `LICENSE` file for details.

