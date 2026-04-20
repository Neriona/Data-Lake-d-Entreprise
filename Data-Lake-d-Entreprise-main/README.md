# 🏭 Data Lake Enterprise - HDFS, Hive, Spark, Airflow

Complete data pipeline for enterprise data lake with ingestion, ETL, quality checks, and orchestration.

## 📋 Architecture

\\\
Sources (ERP, CRM, APIs)
    ↓
Ingestion → Raw Zone (JSON/CSV)
    ↓
ETL Spark → Curated Zone (Parquet)
    ↓
Quality Checks (8 Dimensions)
    ↓
Analytics / BI
\\\

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- 8GB+ RAM available
- 20GB+ disk space

### Startup


# 1. Start all services
docker-compose up -d

# 2. Wait for services to be ready (2-3 minutes)
docker-compose logs -f

# 3. Access UIs
- Airflow: http://localhost:8282 (admin/admin)
- Spark Master: http://localhost:8080
- HDFS NameNode: http://localhost:9870
\\\

### Run Pipeline

\\\ash
# Method 1: Via Airflow UI
# 1. Go to http://localhost:8282
# 2. Find DAG: data_lake_ingestion_and_etl
# 3. Click "Trigger DAG"

# Method 2: Via CLI
docker exec airflow-scheduler airflow dags trigger data_lake_ingestion_and_etl
\\\

### Stop Services

\\\ash
docker-compose down
\\\

## 📁 Project Structure

\\\
.
├── docker-compose.yml         # Docker infrastructure
├── hadoop.env                 # Hadoop configuration
├── dags/
│   └── dag_ingestion.py       # Main DAG orchestration
├── scripts/
│   ├── ingest_to_raw.py       # Data ingestion
│   ├── etl_ventes.py          # Sales ETL
│   ├── etl_clients.py         # Clients ETL
│   ├── compact_curated.py     # Data compaction
│   ├── data_quality_checks.py # Quality validation
│   └── nettoyage_raw.py       # Raw data cleanup
├── hive/
│   └── create_tables.sql      # Hive table definitions
└── data/
    ├── sources/               # Test data
    ├── raw/                   # Ingested data
    └── curated/               # Processed data
\\\

## 🔄 Data Flow

1. **Ingestion** (Personne A)
   - Load CSV from sources → Raw Zone (HDFS)
   - Add technical columns (source_system, timestamp, filename)

2. **ETL** (Personne B)
   - Clean, transform, enrich data
   - Write to Curated Zone (Parquet)

3. **Quality** (Personne C)
   - 8-dimension quality checks
   - Generate reports
   - Alert on failures

## 📊 Services

| Service | Port | URL | Credentials |
|---------|------|-----|-------------|
| Airflow | 8282 | http://localhost:8282 | admin/admin |
| Spark | 8080 | http://localhost:8080 | N/A |
| HDFS | 9870 | http://localhost:9870 | N/A |
| Hive Metastore | 9083 | N/A | N/A |

## 🛠️ Common Commands

\\\ash
# View logs
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master

# Execute Spark command
docker exec spark-master spark-submit /scripts/etl_ventes.py --date 2026-04-19

# Run data quality checks
docker exec airflow-webserver python /opt/airflow/scripts/data_quality_checks.py \
  --zone raw --entity ventes --date 2026-04-19

# Access HDFS
docker exec namenode hdfs dfs -ls /data/raw/ventes/
\\\

## 📝 Troubleshooting

### Airflow not starting
\\\ash
docker-compose logs airflow-webserver
# Check database connection and migrations
\\\

### Spark job failures
\\\ash
docker exec spark-master spark-submit --master spark://spark-master:7077 /scripts/etl_ventes.py --date 2026-04-19
\\\

### HDFS issues
\\\ash
docker exec namenode hdfs fsck /data
docker exec namenode hdfs dfs -du -h /data
\\\

## 👥 Team

- **Ait chettou fatima ezzahra**: Ingestion & HDFS 
- **Jabir yassir**: ETL & Spark 
- **Laznasni Rayhana**: Orchestration & QA 

## 📚 Documentation

- [Cahier des Charges](./CAHIER_DES_CHARGES.md)
- [Architecture Guide](./docs/ARCHITECTURE.md)
- [Deployment Guide](./docs/DEPLOYMENT.md)

## ⚖️ License

Proprietary - Enterprise Data Lake Project
