"""Data Quality Checks - Fixed paths"""
import json, csv, sys, argparse
from pathlib import Path
from datetime import datetime

BASE_DIR = Path("/opt/airflow/data")

def read_csv_data(file_path):
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data = list(reader)
    return data

def check_completeness(data, entity):
    if not data:
        return {"dimension": "Complétude", "status": "SKIP", "score": 0}
    
    total_rows = len(data)
    completeness_by_col = {}
    
    for col in data[0].keys():
        null_count = sum(1 for row in data if not row[col] or row[col].strip() == "")
        completeness = (total_rows - null_count) / total_rows * 100
        completeness_by_col[col] = completeness
    
    avg_completeness = sum(completeness_by_col.values()) / len(completeness_by_col)
    status = "PASS" if avg_completeness >= 95 else "WARNING"
    
    return {
        "dimension": "Complétude",
        "status": status,
        "score": round(avg_completeness, 2),
        "columns": completeness_by_col
    }

def check_uniqueness(data, entity):
    if not data:
        return {"dimension": "Unicité", "status": "SKIP", "score": 0}
    
    key_col = "id_vente" if entity == "ventes" else "id_client"
    if key_col not in data[0]:
        return {"dimension": "Unicité", "status": "SKIP", "score": 0}
    
    ids = [row[key_col] for row in data]
    unique_ids = len(set(ids))
    total_rows = len(data)
    duplicates = total_rows - unique_ids
    
    status = "PASS" if duplicates == 0 else "FAIL"
    
    return {
        "dimension": "Unicité",
        "status": status,
        "score": round(unique_ids / total_rows * 100, 2),
        "total": total_rows,
        "unique": unique_ids,
        "duplicates": duplicates
    }

def check_validity(data, entity):
    if not data:
        return {"dimension": "Validité", "status": "SKIP", "score": 0}
    
    if entity == "ventes":
        valid_count = 0
        for row in data:
            try:
                montant = float(row.get("montant", 0))
                if montant >= 0:
                    valid_count += 1
            except:
                pass
        
        validity_rate = valid_count / len(data) * 100 if data else 0
        status = "PASS" if validity_rate >= 98 else "WARNING"
        
        return {
            "dimension": "Validité",
            "status": status,
            "score": round(validity_rate, 2),
            "check": "montants valides et positifs"
        }
    else:
        return {"dimension": "Validité", "status": "PASS", "score": 100}

def check_timeliness(date_str):
    from datetime import datetime, timedelta
    
    try:
        data_date = datetime.strptime(date_str, "%Y-%m-%d")
        today = datetime.now()
        age = (today - data_date).days
        
        status = "PASS" if age <= 1 else "WARNING"
        return {
            "dimension": "Actualité",
            "status": status,
            "score": 100 if status == "PASS" else 50,
            "age_days": age
        }
    except:
        return {"dimension": "Actualité", "status": "FAIL", "score": 0}

def calculate_global_score(dimensions):
    valid_dims = [d for d in dimensions if d.get("score") is not None and d["score"] > 0]
    if not valid_dims:
        return {"global_score": 0, "status": "CRITICAL", "recommendation": "❌ NO DATA"}
    
    avg_score = sum(d["score"] for d in valid_dims) / len(valid_dims)
    
    if avg_score < 70:
        recommendation = "❌ CRÍTICO - Investir na qualidade"
        status = "CRITICAL"
    elif avg_score < 85:
        recommendation = "⚠️  A MELHORAR"
        status = "WARNING"
    else:
        recommendation = "✅ BOM - Pronto"
        status = "OK"
    
    return {
        "global_score": round(avg_score, 2),
        "status": status,
        "recommendation": recommendation
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--zone", required=True, choices=["raw", "curated"])
    parser.add_argument("--entity", required=True)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    if args.zone == "raw":
        file_path = BASE_DIR / "raw" / args.entity / args.date / f"{args.entity}_enrichi.csv"
    else:
        file_path = BASE_DIR / "curated" / args.entity / "data.csv"

    print(f"\n{'='*70}")
    print(f"DATA QUALITY CHECKS - {args.zone.upper()} / {args.entity.upper()}")
    print(f"{'='*70}\n")

    if not file_path.exists():
        print(f"[WARNING] File not found: {file_path}")
        print("Creating dummy report...\n")
        dimensions = []
    else:
        data = read_csv_data(file_path)
        print(f"✓ File read: {len(data)} rows\n")
        
        dimensions = [
            check_completeness(data, args.entity),
            check_uniqueness(data, args.entity),
            check_validity(data, args.entity),
            check_timeliness(args.date),
        ]

    global_score = calculate_global_score(dimensions)

    print("📊 RESULTS\n")
    for dim in dimensions:
        status_symbol = "✓" if dim["status"] == "PASS" else "⚠" if dim["status"] == "WARNING" else "✗"
        score = dim.get("score", "N/A")
        print(f"{status_symbol} {dim['dimension']:20}: {score}%")

    print(f"\n🎯 GLOBAL SCORE: {global_score['global_score']}/100")
    print(f"   Status: {global_score['status']}")
    print(f"   {global_score['recommendation']}")

    # Save report
    report_dir = BASE_DIR.parent / "reports"
    report_dir.mkdir(exist_ok=True)
    
    report = {
        "metadata": {
            "date": datetime.now().isoformat(),
            "zone": args.zone,
            "entity": args.entity
        },
        "dimensions": dimensions,
        "global_assessment": global_score
    }

    report_path = report_dir / f"dq_report_{args.zone}_{args.entity}_{args.date}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n✓ Report saved: {report_path}\n")

if __name__ == "__main__":
    main()
