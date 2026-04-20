from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================
default_args = {
    "owner"            : "personne_a",
    "retries"          : 3,
    "retry_delay"      : timedelta(minutes=5),
    "email_on_failure" : False,
    "email"            : [],
}

# ============================================================================
# DAG DEFINITION - ORCHESTRATION COMPLETE
# ============================================================================
with DAG(
    dag_id          = "data_lake_ingestion_and_etl",
    description     = "Ingestion des donnees brutes + Transformation ETL vers Curated + Data Quality",
    start_date      = datetime(2026, 1, 1),
    schedule_interval = "@daily",
    catchup         = False,
    default_args    = default_args,
    tags            = ["data_lake", "ingestion", "etl", "quality"],
    max_active_runs = 1,
) as dag:

    # ========================================================================
    # ETAPE 1 : CREATION DES REPERTOIRES HDFS
    # ========================================================================
    create_directories = BashOperator(
        task_id      = "create_directories",
        bash_command = """
            echo "Creating HDFS directories..."
            hdfs dfs -mkdir -p /data/raw/ventes/{{ ds }} || true
            hdfs dfs -mkdir -p /data/raw/clients/{{ ds }} || true
            hdfs dfs -mkdir -p /data/curated/ventes || true
            hdfs dfs -mkdir -p /data/curated/clients || true
            echo "✔ Directories created successfully"
        """,
    )

    # ========================================================================
    # ETAPE 2 : INGESTION VENTES (Zone Raw)
    # ========================================================================
    ingest_ventes = BashOperator(
        task_id      = "ingest_ventes",
        bash_command = """
            echo "Ingesting Ventes data..."
            python /opt/airflow/scripts/ingest_to_raw.py {{ ds }} ventes
            echo "✔ Ventes ingestion completed"
        """,
    )

    # ========================================================================
    # ETAPE 3 : INGESTION CLIENTS (Zone Raw)
    # ========================================================================
    ingest_clients = BashOperator(
        task_id      = "ingest_clients",
        bash_command = """
            echo "Ingesting Clients data..."
            python /opt/airflow/scripts/ingest_to_raw.py {{ ds }} clients
            echo "✔ Clients ingestion completed"
        """,
    )

    # ========================================================================
    # ETAPE 4 : QUALITY CHECK RAW VENTES
    # ========================================================================
    quality_check_raw_ventes = BashOperator(
        task_id      = "quality_check_raw_ventes",
        bash_command = """
            echo "Running quality checks on Raw Ventes..."
            python /opt/airflow/scripts/data_quality_checks.py \\
                --zone raw \\
                --entity ventes \\
                --date {{ ds }}
            echo "✔ Quality checks Raw Ventes completed"
        """,
    )

    # ========================================================================
    # ETAPE 5 : QUALITY CHECK RAW CLIENTS
    # ========================================================================
    quality_check_raw_clients = BashOperator(
        task_id      = "quality_check_raw_clients",
        bash_command = """
            echo "Running quality checks on Raw Clients..."
            python /opt/airflow/scripts/data_quality_checks.py \\
                --zone raw \\
                --entity clients \\
                --date {{ ds }}
            echo "✔ Quality checks Raw Clients completed"
        """,
    )

    # ========================================================================
    # ETAPE 6 : ETL CLIENTS (Raw → Curated) - MUST RUN FIRST
    # ========================================================================
    etl_clients = BashOperator(
        task_id      = "etl_clients",
        bash_command = """
            echo "Running ETL Clients transformation..."
            spark-submit /opt/airflow/scripts/etl_clients.py --date {{ ds }}
            echo "✔ ETL Clients completed"
        """,
    )

    # ========================================================================
    # ETAPE 7 : ETL VENTES (Raw → Curated) - DEPENDS ON CLIENTS
    # ========================================================================
    etl_ventes = BashOperator(
        task_id      = "etl_ventes",
        bash_command = """
            echo "Running ETL Ventes transformation..."
            spark-submit /opt/airflow/scripts/etl_ventes.py --date {{ ds }}
            echo "✔ ETL Ventes completed"
        """,
    )

    # ========================================================================
    # ETAPE 8 : QUALITY CHECK CURATED VENTES
    # ========================================================================
    quality_check_curated_ventes = BashOperator(
        task_id      = "quality_check_curated_ventes",
        bash_command = """
            echo "Running quality checks on Curated Ventes..."
            python /opt/airflow/scripts/data_quality_checks.py \\
                --zone curated \\
                --entity ventes \\
                --date {{ ds }}
            echo "✔ Quality checks Curated Ventes completed"
        """,
    )

    # ========================================================================
    # ETAPE 9 : QUALITY CHECK CURATED CLIENTS
    # ========================================================================
    quality_check_curated_clients = BashOperator(
        task_id      = "quality_check_curated_clients",
        bash_command = """
            echo "Running quality checks on Curated Clients..."
            python /opt/airflow/scripts/data_quality_checks.py \\
                --zone curated \\
                --entity clients \\
                --date {{ ds }}
            echo "✔ Quality checks Curated Clients completed"
        """,
    )

    # ========================================================================
    # ETAPE 10 : COMPACTION (maintenance)
    # ========================================================================
    compaction = BashOperator(
        task_id      = "compaction_curated",
        bash_command = """
            echo "Running compaction on Curated zones..."
            spark-submit /opt/airflow/scripts/compact_curated.py --entity all
            echo "✔ Compaction completed"
        """,
    )

    # ========================================================================
    # ETAPE 11 : CLEANUP RAW (>30 jours)
    # ========================================================================
    cleanup_raw = BashOperator(
        task_id      = "cleanup_raw_old_data",
        bash_command = """
            echo "Cleaning up raw data older than 30 days..."
            python /opt/airflow/scripts/nettoyage_raw.py
            echo "✔ Cleanup completed"
        """,
    )

    # ========================================================================
    # DEFINIR LES DEPENDANCES (ORCHESTRATION)
    # ========================================================================
    # Etape 1 : Creer repertoires d'abord
    create_directories >> [ingest_ventes, ingest_clients]

    # Etape 2 : Quality checks raw
    ingest_ventes >> quality_check_raw_ventes
    ingest_clients >> quality_check_raw_clients

    # Etape 3 : ETL (clients AVANT ventes car jointure)
    quality_check_raw_clients >> etl_clients
    quality_check_raw_ventes >> etl_ventes
    etl_clients >> etl_ventes

    # Etape 4 : Quality checks curated
    etl_ventes >> quality_check_curated_ventes
    etl_clients >> quality_check_curated_clients

    # Etape 5 : Compaction et cleanup apres tous les checks
    
    # ========================================================================
    # ETAPE 12 : SEND ALERT - SUCCESS NOTIFICATION
    # ========================================================================
    send_alert_success = BashOperator(
        task_id="send_alert_success",
        bash_command="""
            echo "Sending success alert..."
            python /opt/airflow/scripts/send_alert.py DATA_LAKE_SUCCESS data_lake all_zones "Pipeline completed successfully" INFO
            echo "Alert sent"
        """,
    )

    # Add alert to end of pipeline
    [quality_check_curated_ventes, quality_check_curated_clients] >> compaction >> cleanup_raw >> send_alert_success

