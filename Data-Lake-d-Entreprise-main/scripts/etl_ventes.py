"""
=============================================================================
ETL Pipeline - Zone Curated : Ventes
=============================================================================
Auteur      : Personne B – Data Engineer
Version     : 1.0
Date        : 2026-04-14
Description : Lecture de la zone Raw (JSON/CSV), nettoyage, enrichissement
              et écriture Parquet partitionné dans la zone Curated.

Usage :
    spark-submit etl_ventes.py --date 2026-04-14
=============================================================================
"""

import sys
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType
)

# ---------------------------------------------------------------------------
# Configuration du logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("ETL_Ventes")

# ---------------------------------------------------------------------------
# Schéma attendu en zone Raw
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("id_vente",            StringType(),  False),
    StructField("date_vente",          StringType(),  True),
    StructField("montant",             StringType(),  True),
    StructField("produit",             StringType(),  True),
    StructField("client_id",           StringType(),  True),
    StructField("source_system",       StringType(),  True),
    StructField("ingestion_timestamp", StringType(),  True),
    StructField("file_name",           StringType(),  True),
])

TVA_RATE = 1.20


# ---------------------------------------------------------------------------
# Création de la session Spark
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Initialisation de la session Spark : {app_name}")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.sources.bucketing.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# Étape 1 : Lecture Raw
# ---------------------------------------------------------------------------
def read_raw(spark: SparkSession, date: str) -> DataFrame:
    path = f"/data/raw/ventes/{date}/"
    logger.info(f"Lecture des données brutes depuis : {path}")
    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("multiLine", "true")
        .csv(path)
    )
    count = df.count()
    logger.info(f"Lignes lues depuis Raw : {count:,}")
    return df


# ---------------------------------------------------------------------------
# Étape 2 : Nettoyage
# ---------------------------------------------------------------------------
def clean(df: DataFrame) -> DataFrame:
    logger.info("Début du nettoyage...")

    initial_count = df.count()

    # Suppression des doublons sur clé métier
    df = df.dropDuplicates(["id_vente"])
    logger.info(f"Doublons supprimés : {initial_count - df.count():,}")

    # Filtrage des lignes sans clé métier
    df = df.filter(F.col("id_vente").isNotNull() & (F.col("id_vente") != ""))

    # Filtrage montant nul ou négatif
    df = df.withColumn("montant", F.col("montant").cast(DoubleType()))
    df = df.filter(F.col("montant").isNotNull() & (F.col("montant") > 0))

    # Conversion des types
    df = df.withColumn("quantite",   F.col("quantite").cast(IntegerType()))
    df = df.withColumn("date_vente", F.to_date(F.col("date_vente"), "yyyy-MM-dd"))

    # Nettoyage des chaînes
    df = df.withColumn("produit", F.trim(F.upper(F.col("produit"))))
    df = df.withColumn("client_id", F.trim(F.col("client_id")))

    # Valeurs par défaut
    df = df.fillna({"produit": "INCONNU", "client_id": "UNKNOWN"})
    
    # Ajouter colonne quantite (par défaut 1 puisqu'elle n'existe pas dans les données)
    df = df.withColumn("quantite", F.lit(1).cast(IntegerType()))
    final_count = df.count()
    logger.info(f"Lignes après nettoyage : {final_count:,}")
    return df


# ---------------------------------------------------------------------------
# Étape 3 : Enrichissement
# ---------------------------------------------------------------------------
def enrich(df: DataFrame, spark: SparkSession) -> DataFrame:
    logger.info("Début de l'enrichissement...")

    # Jointure avec la table clients (zone curated)
    try:
        clients = spark.read.parquet("/data/curated/clients/") \
            .select("id_client", "segment", "region_client") \
            .dropDuplicates(["id_client"])
        df = df.join(clients, on="id_client", how="left")
        logger.info("Jointure avec curated/clients effectuée.")
    except Exception as e:
        logger.warning(f"Jointure clients impossible (table absente ?) : {e}")
        df = df.withColumn("segment",       F.lit(None).cast(StringType()))
        df = df.withColumn("region_client", F.lit(None).cast(StringType()))

    # Colonnes calculées
    df = df.withColumn("montant_ttc",     F.round(F.col("montant") * TVA_RATE, 2))
    df = df.withColumn("montant_total",   F.round(F.col("montant") * F.col("quantite"), 2))
    df = df.withColumn("montant_total_ttc", F.round(F.col("montant_ttc") * F.col("quantite"), 2))

    # Colonnes de partitionnement
    df = df.withColumn("year",  F.year("date_vente").cast(StringType()))
    df = df.withColumn("month", F.lpad(F.month("date_vente").cast(StringType()), 2, "0"))
    df = df.withColumn("day",   F.lpad(F.dayofmonth("date_vente").cast(StringType()), 2, "0"))

    return df


# ---------------------------------------------------------------------------
# Étape 4 : Métadonnées
# ---------------------------------------------------------------------------
def add_metadata(df: DataFrame, date: str) -> DataFrame:
    return df \
        .withColumn("_last_transformed", F.lit(datetime.now().isoformat())) \
        .withColumn("_etl_version",      F.lit("1.0")) \
        .withColumn("_source_date",      F.lit(date))


# ---------------------------------------------------------------------------
# Étape 5 : Écriture Curated (idempotente)
# ---------------------------------------------------------------------------
def write_curated(df: DataFrame, entity: str) -> None:
    output_path = f"/data/curated/{entity}/"
    logger.info(f"Écriture Parquet vers : {output_path}")
    logger.info("Partitionnement : year/month/day")
    logger.info("Format : Parquet avec compression Snappy")

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info(f" Écriture terminée : {entity} → {output_path}")


# ---------------------------------------------------------------------------
# Vues temporaires (usage test / debug)
# ---------------------------------------------------------------------------
def run_sanity_checks(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("ventes_curated_tmp")
    logger.info("--- Sanity checks ---")
    spark.sql("SELECT COUNT(*) AS total_lignes FROM ventes_curated_tmp").show()
    spark.sql("SELECT year, month, COUNT(*) AS cnt FROM ventes_curated_tmp GROUP BY year, month ORDER BY year, month").show()
    spark.sql("SELECT region, ROUND(SUM(montant_total_ttc),2) AS ca_ttc FROM ventes_curated_tmp GROUP BY region ORDER BY ca_ttc DESC").show()
    null_check = spark.sql("SELECT COUNT(*) FROM ventes_curated_tmp WHERE id_vente IS NULL").collect()[0][0]
    logger.info(f"Lignes avec id_vente NULL : {null_check}")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="ETL Ventes – Raw → Curated")
    parser.add_argument("--date", required=True, help="Date de traitement (YYYY-MM-DD)")
    args = parser.parse_args()
    date = args.date

    logger.info(f"=== Démarrage ETL Ventes pour la date : {date} ===")

    spark = create_spark_session("ETL_Ventes")
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw      = read_raw(spark, date)
        df_clean    = clean(df_raw)
        df_enriched = enrich(df_clean, spark)
        df_final    = add_metadata(df_enriched, date)

        run_sanity_checks(spark, df_final)
        write_curated(df_final, "ventes")

        logger.info(f"=== ETL Ventes terminé avec succès pour {date} ===")
    except Exception as e:
        logger.error(f"Échec de l'ETL Ventes : {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
