"""Raw data cleanup - Fixed paths"""
import os
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path("/opt/airflow/data")
RAW_DIR = BASE_DIR / "raw"
LIMITE_JOURS = 30

print(f"\n{'='*50}")
print(f"CLEANUP - Removing data older than {LIMITE_JOURS} days")
print(f"{'='*50}\n")

try:
    if not RAW_DIR.exists():
        print(f"[INFO] RAW directory not found: {RAW_DIR}")
        print("Nothing to cleanup\n")
    else:
        limite_date = datetime.now() - timedelta(days=LIMITE_JOURS)
        print(f"Cutoff date: {limite_date.strftime('%Y-%m-%d')}")
        print(f"RAW dir: {RAW_DIR}\n")
        
        fichiers_supprimes = 0
        
        for fichier in RAW_DIR.rglob("*.csv"):
            file_date = datetime.fromtimestamp(fichier.stat().st_mtime)
            if file_date < limite_date:
                fichier.unlink()
                print(f"  ✓ Deleted: {fichier.relative_to(RAW_DIR)}")
                fichiers_supprimes += 1
        
        print(f"\n✓ Cleanup completed: {fichiers_supprimes} files removed\n")

except Exception as e:
    print(f"❌ Error: {e}\n")
    import traceback
    traceback.print_exc()
