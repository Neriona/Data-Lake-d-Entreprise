-- ============================================================================
-- Base de données principale du Data Lake
-- ============================================================================
CREATE DATABASE IF NOT EXISTS data_lake
COMMENT 'Base de données principale du Data Lake d Enterprise';

USE data_lake;

-- ============================================================================
-- TABLE VENTES - Zone Curated
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ventes (
    id_vente            STRING  COMMENT 'Identifiant unique de la vente',
    date_vente          STRING  COMMENT 'Date de la vente (YYYY-MM-DD)',
    montant             DOUBLE  COMMENT 'Montant de la vente en euros (HT)',
    produit             STRING  COMMENT 'Nom du produit vendu',
    client_id           STRING  COMMENT 'Identifiant du client',
    source_system       STRING  COMMENT 'Système source de la donnée',
    ingestion_timestamp STRING  COMMENT 'Date et heure d ingestion UTC',
    file_name           STRING  COMMENT 'Nom du fichier source',
    montant_ttc         DOUBLE  COMMENT 'Montant TTC (montant * 1.20)',
    montant_total       DOUBLE  COMMENT 'Montant total avant TVA (montant * quantite)',
    montant_total_ttc   DOUBLE  COMMENT 'Montant total TTC',
    quantite            INT     COMMENT 'Quantité vendue',
    segment             STRING  COMMENT 'Segment client (jointure clients)',
    region_client       STRING  COMMENT 'Région client (jointure clients)',
    _last_transformed   STRING  COMMENT 'Date/heure dernière transformation',
    _etl_version        STRING  COMMENT 'Version du pipeline ETL',
    _source_date        STRING  COMMENT 'Date source du fichier ingéré'
)
PARTITIONED BY (
    year  STRING COMMENT 'Année (YYYY)',
    month STRING COMMENT 'Mois (MM)',
    day   STRING COMMENT 'Jour (DD)'
)
STORED AS PARQUET
LOCATION '/data/curated/ventes/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'EXTERNAL'='TRUE'
);

-- ============================================================================
-- TABLE CLIENTS - Zone Curated
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS clients (
    id_client           STRING  COMMENT 'Identifiant unique du client',
    nom                 STRING  COMMENT 'Nom du client (SHA256 hashed - RGPD)',
    prenom              STRING  COMMENT 'Prénom du client (SHA256 hashed - RGPD)',
    email               STRING  COMMENT 'Email du client (SHA256 hashed - RGPD)',
    telephone           STRING  COMMENT 'Téléphone du client (SHA256 hashed - RGPD)',
    date_naissance      STRING  COMMENT 'Date de naissance (SHA256 hashed - RGPD)',
    date_inscription    STRING  COMMENT 'Date d inscription (YYYY-MM-DD)',
    segment             STRING  COMMENT 'Segment client (STANDARD, PREMIUM)',
    region_client       STRING  COMMENT 'Région du client',
    actif               BOOLEAN COMMENT 'Statut actif (true/false)',
    code_postal         STRING  COMMENT 'Code postal',
    age                 INT     COMMENT 'Âge calculé à partir date_naissance',
    tranche_age         STRING  COMMENT 'Tranche d âge (18-25, 26-35, etc.)',
    anciennete_jours    INT     COMMENT 'Ancienneté en jours depuis inscription',
    statut_fidelite     STRING  COMMENT 'Statut fidélité (BRONZE, SILVER, GOLD)',
    source_system       STRING  COMMENT 'Système source',
    ingestion_timestamp STRING  COMMENT 'Date et heure d ingestion',
    file_name           STRING  COMMENT 'Nom du fichier source',
    _last_transformed   STRING  COMMENT 'Date/heure dernière transformation',
    _etl_version        STRING  COMMENT 'Version du pipeline ETL',
    _source_date        STRING  COMMENT 'Date source du fichier ingéré'
)
PARTITIONED BY (
    region_client STRING COMMENT 'Région (pour partitionnement)',
    segment       STRING COMMENT 'Segment (pour partitionnement)'
)
STORED AS PARQUET
LOCATION '/data/curated/clients/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'EXTERNAL'='TRUE'
);