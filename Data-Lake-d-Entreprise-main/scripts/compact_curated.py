"""
=============================================================================
Script de Compaction – Zone Curated
=============================================================================
Auteur      : Personne B – Data Engineer
Version     : 1.1
Date        : 2026-04-14
Description : Regroupe les petits fichiers Parquet de la zone Curated en
              fichiers de taille optimale pour améliorer les performances
              de lecture (Hive, Spark).

              À exécuter en maintenance hebdomadaire ou via un DAG Airflow
              dédié.

Usage :
    spark-submit compact_curated.py --entity ventes --partitions 20
    spark-submit compact_curated.py --entity clients --partitions 10
    spark-submit compact_curated.py --entity all
=============================================================================
"""

import sys
import argparse
import logging
import subprocess
from typing import List

from pyspark.sql import SparkSession, DataFrame

# ---------------------------------------------------------------------------
# Configuration du logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("Compaction")

# ---------------------------------------------------------------------------
# Entités gérées et config de compaction
# ---------------------------------------------------------------------------
ENTITIES_CONFIG = {
    "ventes": {
        "partition_cols": ["year", "month", "day"],
        "target_files":   20,
        "target_file_mb": 256,
    },
    "clients": {
        "partition_cols": ["region_client", "segment"],
        "target_files":   10,
        "target_file_mb": 128,
    },
}


# ---------------------------------------------------------------------------
# Création de la session Spark
# ---------------------------------------------------------------------------
def create_spark_session(app_name: str) -> SparkSession:
    logger.info(f"Initialisation de la session Spark : {app_name}")
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Vérification de l'existence d'un chemin HDFS
# ---------------------------------------------------------------------------
def path_exists_hdfs(path: str) -> bool:
    """Vérifie si un chemin HDFS existe."""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", path],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout lors de la vérification du chemin : {path}")
        return False
    except Exception as e:
        logger.warning(f"Erreur lors de la vérification du chemin {path} : {e}")
        return False


# ---------------------------------------------------------------------------
# Lecture du Parquet existant
# ---------------------------------------------------------------------------
def read_curated(spark: SparkSession, entity: str) -> DataFrame:
    path = f"/data/curated/{entity}/"
    logger.info(f"Lecture des données depuis : {path}")
    return spark.read.parquet(path)


# ---------------------------------------------------------------------------
# Calcul de la taille totale via HDFS
# ---------------------------------------------------------------------------
def get_hdfs_size_mb(path: str) -> float:
    """Retourne la taille totale du chemin HDFS en MB."""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-du", "-s", path],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            size_bytes = int(result.stdout.split()[0])
            size_mb = round(size_bytes / (1024 ** 2), 2)
            logger.info(f"Taille HDFS de {path} : {size_mb} MB")
            return size_mb
    except Exception as e:
        logger.warning(f"Impossible de lire la taille HDFS : {e}")
    return 0.0


# ---------------------------------------------------------------------------
# Calcul du nombre de fichiers cible selon la taille
# ---------------------------------------------------------------------------
def compute_target_partitions(size_mb: float, target_file_mb: int, default: int) -> int:
    """Calcule le nombre de fichiers cible basé sur la taille."""
    if size_mb > 0:
        n = max(1, int(size_mb / target_file_mb))
        logger.info(f"Taille détectée : {size_mb} MB → {n} fichiers cible")
        return n
    logger.info(f"Taille non détectée, utilisation de la valeur par défaut : {default}")
    return default


# ---------------------------------------------------------------------------
# Exécution de commande HDFS avec timeout et gestion d'erreur
# ---------------------------------------------------------------------------
def _hdfs_run(cmd: List[str], max_retries: int = 3) -> None:
    """Exécute une commande HDFS avec timeout et retry."""
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Exécution HDFS (tentative {attempt}/{max_retries}) : {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes max
            )
            if result.returncode == 0:
                logger.info(f"✅ HDFS : {' '.join(cmd)} → OK")
                return
            else:
                logger.error(f"Erreur HDFS (code {result.returncode}) : {result.stderr}")
                if attempt < max_retries:
                    logger.info(f"Retry dans 5 secondes...")
                    import time
                    time.sleep(5)
                else:
                    raise RuntimeError(f"HDFS failed after {max_retries} attempts: {result.stderr}")
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout HDFS (> 300s) : {' '.join(cmd)}")
            if attempt < max_retries:
                logger.info(f"Retry dans 5 secondes...")
                import time
                time.sleep(5)
            else:
                raise RuntimeError(f"HDFS timeout after {max_retries} attempts: {' '.join(cmd)}")


# ---------------------------------------------------------------------------
# Compaction d'une entité
# ---------------------------------------------------------------------------
def compact_entity(spark: SparkSession, entity: str, forced_partitions: int = None) -> None:
    """Compacte les fichiers Parquet d'une entité."""
    config = ENTITIES_CONFIG.get(entity)
    if config is None:
        raise ValueError(
            f"Entité inconnue : '{entity}'. "
            f"Entités disponibles : {list(ENTITIES_CONFIG.keys())}"
        )

    curated_path = f"/data/curated/{entity}/"
    tmp_path     = f"/data/curated/{entity}_compact_tmp/"
    backup_path  = f"/data/curated/{entity}_backup/"

    logger.info(f"\n{'='*70}")
    logger.info(f"=== COMPACTION DE : {entity.upper()} ===")
    logger.info(f"{'='*70}")

    # Vérifier que le chemin existe
    if not path_exists_hdfs(curated_path):
        logger.warning(f"⚠️  Chemin inexistant : {curated_path}")
        logger.warning(f"   Aucune donnée à compacter pour '{entity}'")
        return

    # Taille HDFS actuelle
    size_mb_before = get_hdfs_size_mb(curated_path)

    # Nombre de fichiers cible
    if forced_partitions:
        target_n = forced_partitions
        logger.info(f"Nombre de fichiers cible (forcé) : {target_n}")
    else:
        target_n = compute_target_partitions(
            size_mb_before,
            config["target_file_mb"],
            config["target_files"]
        )

    # Lecture
    logger.info(f"Lecture des données depuis {curated_path}...")
    df = read_curated(spark, entity)
    original_count = df.count()
    partitions_before = df.rdd.getNumPartitions()
    
    logger.info(f"✓ Lignes lues : {original_count:,}")
    logger.info(f"✓ Partitions Spark avant : {partitions_before}")

    # Coalesce
    logger.info(f"Coalescing vers {target_n} partition(s)...")
    df_compacted = df.coalesce(target_n)
    partitions_after_coalesce = df_compacted.rdd.getNumPartitions()
    logger.info(f"✓ Partitions Spark après coalesce : {partitions_after_coalesce}")

    # Écriture dans dossier temporaire
    logger.info(f"Écriture temporaire dans : {tmp_path}")
    (
        df_compacted.write
        .mode("overwrite")
        .partitionBy(*config["partition_cols"])
        .parquet(tmp_path)
    )
    logger.info(f"✓ Écriture temporaire réussie")

    # Vérification de l'intégrité
    logger.info(f"Vérification de l'intégrité...")
    df_check = spark.read.parquet(tmp_path)
    compact_count = df_check.count()

    if compact_count != original_count:
        logger.error(f"❌ Intégrité KO :")
        logger.error(f"   Lignes initiales : {original_count:,}")
        logger.error(f"   Lignes après compaction : {compact_count:,}")
        logger.error(f"   ANNULATION - données compactées préservées dans : {tmp_path}")
        raise RuntimeError(
            f"Data integrity check failed: "
            f"{original_count:,} → {compact_count:,}"
        )
    logger.info(f"✅ Intégrité OK : {compact_count:,} lignes confirmées")

    # Taille après compaction
    size_mb_after_compact = get_hdfs_size_mb(tmp_path)
    reduction = round(100 * (1 - size_mb_after_compact / size_mb_before), 2) if size_mb_before > 0 else 0
    logger.info(f"✓ Réduction de taille : {reduction}% ({size_mb_before} MB → {size_mb_after_compact} MB)")

    # Rotation HDFS
    _hdfs_rotate(curated_path, backup_path, tmp_path)

    logger.info(f"\n{'='*70}")
    logger.info(f"✅ Compaction terminée avec succès pour '{entity}'")
    logger.info(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Rotation HDFS atomique (backup + remplacement)
# ---------------------------------------------------------------------------
def _hdfs_rotate(curated_path: str, backup_path: str, tmp_path: str) -> None:
    """Effectue une rotation HDFS atomique avec rollback en cas d'erreur."""
    logger.info(f"\n--- Rotation HDFS ---")
    
    try:
        # Étape 1 : Supprimer le backup précédent
        logger.info(f"[1/3] Suppression du backup précédent : {backup_path}")
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", "-f", backup_path],
            capture_output=True,
            text=True,
            timeout=300
        )
        logger.info(f"      ✓ Backup ancien supprimé")

        # Étape 2 : Sauvegarder le curated actuel
        logger.info(f"[2/3] Sauvegarde du curated actuel : {curated_path} → {backup_path}")
        _hdfs_run(["hdfs", "dfs", "-mv", curated_path, backup_path])
        logger.info(f"      ✓ Données originales sauvegardées")

        # Étape 3 : Remplacer par le compacté
        logger.info(f"[3/3] Déplacement du compacté : {tmp_path} → {curated_path}")
        _hdfs_run(["hdfs", "dfs", "-mv", tmp_path, curated_path])
        logger.info(f"      ✓ Données compactées activées")

        logger.info(f"\n✅ Rotation HDFS réussie")
        logger.info(f"   Backup disponible à : {backup_path}")

    except Exception as e:
        logger.error(f"\n❌ ERREUR LORS DE LA ROTATION HDFS")
        logger.error(f"   Erreur : {e}")
        logger.error(f"\n   ⚠️  SITUATION CRITIQUE - ACTION MANUELLE REQUISE ⚠️")
        logger.error(f"   - Vérifier l'état de {curated_path}")
        logger.error(f"   - Données compactées possiblement à : {tmp_path}")
        logger.error(f"   - Données originales possiblement à : {backup_path}")
        logger.error(f"\n   CONTACT : équipe DevOps")
        raise RuntimeError(
            f"HDFS rotation failed - data consistency may be compromised. "
            f"Check {backup_path} and {tmp_path}"
        )


# ---------------------------------------------------------------------------
# Suppression des backups anciens
# ---------------------------------------------------------------------------
def cleanup_backups(entities: List[str], dry_run: bool = True) -> None:
    """Supprime les backups de compaction."""
    logger.info(f"\n--- Nettoyage des backups ---")
    for entity in entities:
        backup_path = f"/data/curated/{entity}_backup/"
        if dry_run:
            logger.info(f"[DRY RUN] Suppression simulée : {backup_path}")
        else:
            logger.info(f"Suppression du backup : {backup_path}")
            subprocess.run(
                ["hdfs", "dfs", "-rm", "-r", "-f", backup_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            logger.info(f"✓ Backup supprimé : {backup_path}")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Compaction – Zone Curated Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  spark-submit compact_curated.py --entity ventes
  spark-submit compact_curated.py --entity all
  spark-submit compact_curated.py --entity clients --partitions 10
  spark-submit compact_curated.py --entity all --cleanup-backups
        """
    )
    parser.add_argument(
        "--entity",
        required=True,
        help=f"Entité à compacter : {', '.join(ENTITIES_CONFIG.keys())} ou 'all'"
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Nombre de fichiers cible (optionnel, sinon calculé automatiquement)"
    )
    parser.add_argument(
        "--cleanup-backups",
        action="store_true",
        help="Supprimer les backups après compaction réussie (default: dry-run)"
    )
    args = parser.parse_args()

    spark = create_spark_session("Compaction_Curated")
    spark.sparkContext.setLogLevel("WARN")

    # Valider entity
    if args.entity == "all":
        entities_to_process = list(ENTITIES_CONFIG.keys())
    elif args.entity in ENTITIES_CONFIG:
        entities_to_process = [args.entity]
    else:
        logger.error(f"❌ Entité inconnue : '{args.entity}'")
        logger.error(f"   Entités disponibles : {', '.join(ENTITIES_CONFIG.keys())} ou 'all'")
        sys.exit(1)

    logger.info(f"\n{'='*70}")
    logger.info(f"COMPACTION CURATED - Début")
    logger.info(f"{'='*70}")
    logger.info(f"Entités à compacter : {', '.join(entities_to_process)}")
    logger.info(f"Cleanup backups : {'✓' if args.cleanup_backups else '✗ (dry-run)'}")
    logger.info(f"{'='*70}\n")

    errors = []
    for entity in entities_to_process:
        try:
            compact_entity(spark, entity, forced_partitions=args.partitions)
        except Exception as e:
            logger.error(f"❌ Échec compaction '{entity}' : {e}", exc_info=True)
            errors.append(entity)

    # Cleanup backups si demandé et pas d'erreurs
    if args.cleanup_backups and not errors:
        cleanup_backups(entities_to_process, dry_run=False)
    elif args.cleanup_backups and errors:
        logger.warning(f"Cleanup backups ANNULÉ (erreurs détectées)")
        logger.info(f"Relancer avec --cleanup-backups une fois les erreurs corrigées")

    spark.stop()

    logger.info(f"\n{'='*70}")
    if errors:
        logger.error(f"COMPACTION TERMINÉE AVEC ERREURS")
        logger.error(f"Entités en erreur : {', '.join(errors)}")
        logger.error(f"{'='*70}\n")
        sys.exit(1)
    else:
        logger.info(f"✅ COMPACTION GLOBALE RÉUSSIE")
        logger.info(f"{'='*70}\n")


if __name__ == "__main__":
    main()