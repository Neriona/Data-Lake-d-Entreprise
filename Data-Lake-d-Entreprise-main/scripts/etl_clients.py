"""
=============================================================================
ETL Pipeline - Zone Curated : Clients
=============================================================================
Auteur      : Personne B – Data Engineer
Version     : 1.0
Date        : 2026-04-14
Description : Lecture de la zone Raw (JSON/CSV), nettoyage, enrichissement
              et écriture Parquet partitionné dans la zone Curated.

Usage :
    spark-submit etl_clients.py --date 2026-04-14
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
    StringType, IntegerType, DateType, BooleanType
)

# ---------------------------------------------------------------------------
# Configuration du logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("ETL_Clients")

# ---------------------------------------------------------------------------
# Schéma attendu en zone Raw
# ---------------------------------------------------------------------------
RAW_SCHEMA = StructType([
    StructField("id_client",        StringType(),  False),
    StructField("nom",              StringType(),  True),
    StructField("prenom",           StringType(),  True),
    StructField("email",            StringType(),  True),
    StructField("telephone",        StringType(),  True),
    StructField("date_naissance",   StringType(),  True),
    StructField("date_inscription", StringType(),  True),
    StructField("segment",          StringType(),  True),
    StructField("region_client",    StringType(),  True),
    StructField("actif",            StringType(),  True),
    StructField("code_postal",      StringType(),  True),
])

# Seuils de qualité
MIN_AGE = 18
MAX_AGE = 120


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
        # Bucketing : nécessaire pour que Spark respecte les buckets à la lecture
        .config("spark.sql.sources.bucketing.enabled", "true")
        # Désactive le broadcast join pour forcer le bucket join avec ventes
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .enableHiveSupport()
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Étape 1 : Lecture Raw
# ---------------------------------------------------------------------------
def read_raw(spark: SparkSession, date: str) -> DataFrame:
    path = f"/data/raw/clients/{date}/"
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
    df = df.dropDuplicates(["id_client"])
    logger.info(f"Doublons supprimés : {initial_count - df.count():,}")

    # Filtrage : clé métier obligatoire
    df = df.filter(F.col("id_client").isNotNull() & (F.col("id_client") != ""))

    # Conversion des types temporels
    df = df.withColumn("date_naissance",   F.to_date(F.col("date_naissance"),   "yyyy-MM-dd"))
    df = df.withColumn("date_inscription", F.to_date(F.col("date_inscription"), "yyyy-MM-dd"))

    # Calcul et validation de l'âge
    today = F.current_date()
    df = df.withColumn(
        "age",
        F.floor(F.datediff(today, F.col("date_naissance")) / 365.25).cast(IntegerType())
    )
    df = df.filter(
        F.col("date_naissance").isNull() |
        ((F.col("age") >= MIN_AGE) & (F.col("age") <= MAX_AGE))
    )

    # Nettoyage / normalisation des chaînes
    df = df.withColumn("nom",           F.trim(F.upper(F.col("nom"))))
    df = df.withColumn("prenom",        F.trim(F.initcap(F.col("prenom"))))
    df = df.withColumn("email",         F.trim(F.lower(F.col("email"))))
    df = df.withColumn("region_client", F.trim(F.upper(F.col("region_client"))))
    df = df.withColumn("segment",       F.trim(F.upper(F.col("segment"))))
    df = df.withColumn("code_postal",   F.trim(F.col("code_postal")))

    # Validation format email (simple)
    df = df.withColumn(
        "email",
        F.when(F.col("email").rlike(r"^[\w.+-]+@[\w-]+\.[a-z]{2,}$"), F.col("email"))
         .otherwise(F.lit(None))
    )

    # Nettoyage téléphone : garder uniquement chiffres et +
    df = df.withColumn("telephone", F.regexp_replace(F.col("telephone"), r"[^\d+]", ""))

    # Conversion booléen actif
    df = df.withColumn(
        "actif",
        F.when(F.lower(F.col("actif")).isin("true", "1", "oui", "yes"), F.lit(True))
         .when(F.lower(F.col("actif")).isin("false", "0", "non", "no"), F.lit(False))
         .otherwise(F.lit(True))  # actif par défaut
        .cast(BooleanType())
    )

    # Valeurs par défaut
    df = df.fillna({
        "segment":       "STANDARD",
        "region_client": "INCONNU",
    })

    final_count = df.count()
    logger.info(f"Lignes après nettoyage : {final_count:,}")
    return df


# ---------------------------------------------------------------------------
# Étape 3 : Enrichissement
# ---------------------------------------------------------------------------
def enrich(df: DataFrame) -> DataFrame:
    logger.info("Début de l'enrichissement...")

    # Tranche d'âge
    df = df.withColumn(
        "tranche_age",
        F.when(F.col("age") < 26,  F.lit("18-25"))
         .when(F.col("age") < 36,  F.lit("26-35"))
         .when(F.col("age") < 51,  F.lit("36-50"))
         .when(F.col("age") < 66,  F.lit("51-65"))
         .otherwise(F.lit("65+"))
    )

    # Ancienneté en jours
    df = df.withColumn(
        "anciennete_jours",
        F.datediff(F.current_date(), F.col("date_inscription")).cast(IntegerType())
    )

    # Statut fidélité basé sur ancienneté
    df = df.withColumn(
        "statut_fidelite",
        F.when(F.col("anciennete_jours") >= 365 * 3, F.lit("GOLD"))
         .when(F.col("anciennete_jours") >= 365,     F.lit("SILVER"))
         .otherwise(F.lit("BRONZE"))
    )

    # Colonnes de partitionnement (année d'inscription)
    df = df.withColumn(
        "year_inscription",
        F.year("date_inscription").cast(StringType())
    )
    df = df.withColumn(
        "month_inscription",
        F.lpad(F.month("date_inscription").cast(StringType()), 2, "0")
    )

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
    logger.info("⚠️  PII Masking (RGPD) : email, telephone, nom, prenom, date_naissance → SHA256")
    logger.info("Partitionnement : region_client/segment")

    # Masquage des colonnes PII (RGPD)
    sensitive_cols = ["email", "telephone", "nom", "prenom", "date_naissance"]
    df_safe = df
    for col in sensitive_cols:
        if col in df.columns:
            df_safe = df_safe.withColumn(col, F.sha2(F.col(col).cast(StringType()), 256))

    (
        df_safe.write
        .mode("overwrite")
        .partitionBy("region_client", "segment")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info(f"✅ Écriture terminée : {entity} → {output_path}")
# ---------------------------------------------------------------------------
# Vues temporaires (usage test / debug)
# ---------------------------------------------------------------------------
def run_sanity_checks(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("clients_curated_tmp")
    logger.info("--- Sanity checks ---")
    spark.sql("SELECT COUNT(*) AS total_clients FROM clients_curated_tmp").show()
    spark.sql("SELECT segment, COUNT(*) AS cnt FROM clients_curated_tmp GROUP BY segment ORDER BY cnt DESC").show()
    spark.sql("SELECT statut_fidelite, COUNT(*) AS cnt FROM clients_curated_tmp GROUP BY statut_fidelite").show()
    spark.sql("SELECT tranche_age, COUNT(*) AS cnt FROM clients_curated_tmp GROUP BY tranche_age ORDER BY tranche_age").show()
    spark.sql("SELECT actif, COUNT(*) AS cnt FROM clients_curated_tmp GROUP BY actif").show()
    null_check = spark.sql("SELECT COUNT(*) FROM clients_curated_tmp WHERE id_client IS NULL").collect()[0][0]
    logger.info(f"Lignes avec id_client NULL : {null_check}")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="ETL Clients – Raw → Curated")
    parser.add_argument("--date", required=True, help="Date de traitement (YYYY-MM-DD)")
    args = parser.parse_args()
    date = args.date

    logger.info(f"=== Démarrage ETL Clients pour la date : {date} ===")

    spark = create_spark_session("ETL_Clients")
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_raw      = read_raw(spark, date)
        df_clean    = clean(df_raw)
        df_enriched = enrich(df_clean)
        df_final    = add_metadata(df_enriched, date)

        run_sanity_checks(spark, df_final)
        write_curated(df_final, "clients")

        logger.info(f"=== ETL Clients terminé avec succès pour {date} ===")
    except Exception as e:
        logger.error(f"Échec de l'ETL Clients : {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
