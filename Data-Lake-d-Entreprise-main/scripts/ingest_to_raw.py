"""Ingestion script - Fixed paths"""
import os, sys, csv
from datetime import datetime
from pathlib import Path

DATE = sys.argv[1] if len(sys.argv) > 1 else datetime.today().strftime("%Y-%m-%d")
SOURCE_NAME = sys.argv[2] if len(sys.argv) > 2 else "ventes"

BASE_DIR = Path("/opt/airflow/data")
LOCAL_DIR = BASE_DIR / "sources"
RAW_DIR = BASE_DIR / "raw" / SOURCE_NAME / DATE

print(f"\n{'='*70}\nINGESTION - {SOURCE_NAME} - {DATE}\n{'='*70}\n")

try:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Directory: {RAW_DIR}\n")
    
    fichiers = [f for f in LOCAL_DIR.glob("*.csv") if "_enrichi" not in f.name]
    
    if not fichiers:
        print(f"[WARNING] No CSV found in {LOCAL_DIR}")
        sys.exit(0)
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for fichier in fichiers:
        print(f"Processing: {fichier.name}")
        with open(fichier, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            lignes = list(reader)
            colonnes = list(reader.fieldnames) if reader.fieldnames else []
        
        if not lignes:
            continue
        
        for ligne in lignes:
            ligne["source_system"] = SOURCE_NAME
            ligne["ingestion_timestamp"] = timestamp
            ligne["file_name"] = fichier.name
        
        fichier_enrichi = RAW_DIR / f"{fichier.stem}_enrichi.csv"
        nouvelles_colonnes = colonnes + ["source_system", "ingestion_timestamp", "file_name"]
        
        with open(fichier_enrichi, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=nouvelles_colonnes)
            writer.writeheader()
            writer.writerows(lignes)
        
        print(f"  ✓ {fichier_enrichi.name} ({len(lignes)} rows)")
    
    print(f"\n✓ Ingestion completed!\n")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
