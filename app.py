# FULL app.py (send only delinquent tenants randomly, limit preview and GPT input to 5)

from flask import Flask, render_template, request, send_file, jsonify
import pandas as pd
import os
import json
import sqlite3
import requests
import random
from werkzeug.utils import secure_filename
from datetime import datetime

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
MERGED_FOLDER = "merged_data"
DB_FILE = "insights.db"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(MERGED_FOLDER, exist_ok=True)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MERGED_FOLDER"] = MERGED_FOLDER

LAMBDA_URL = "https://zv54onyhgk.execute-api.us-west-1.amazonaws.com/prod/insight"

# ---- DATABASE SETUP ----

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenant_insights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                tenant_name TEXT,
                property TEXT,
                insight_json TEXT
            )
        """)

init_db()

# ---- HELPERS ----

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "csv"

def clean_data(df):
    return df.dropna(how="all").reset_index(drop=True)

def safe_float(val):
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return 0.0

def convert_name(name):
    if pd.isna(name): return name
    if "," in name:
        parts = name.split(",", 1)
        return parts[1].strip() + " " + parts[0].strip()
    return name

def merge_datasets(delinquency_path, rent_roll_path, tenant_directory_path):
    columns_needed = [
        "Tenant", "Unit", "Property", "Rent", "Market Rent", "Past Due", "Deposit",
        "Lease To", "Move-in", "Late Count", "Tenure_Months", "Late_Payment_Rate",
        "Delinquent Rent", "Delinquency Notes", "0-30", "30-60", "60-90", "90+",
        "Delinquent Subsidy Amount"
    ]

    delinquency_df = pd.read_csv(delinquency_path)
    rent_roll_df = pd.read_csv(rent_roll_path)
    tenant_directory_df = pd.read_csv(tenant_directory_path)

    delinquency_df = clean_data(delinquency_df)
    rent_roll_df = clean_data(rent_roll_df)
    tenant_directory_df = clean_data(tenant_directory_df)

    tenant_directory_df["Tenant"] = tenant_directory_df["Tenant"].apply(convert_name)
    delinquency_df["Tenant"] = delinquency_df["Name"].apply(convert_name)

    if "Status" in tenant_directory_df.columns:
        tenant_directory_df = tenant_directory_df[tenant_directory_df["Status"] == "Current"]
    if "Status" in rent_roll_df.columns:
        rent_roll_df = rent_roll_df[rent_roll_df["Status"] == "Current"]

    if "Move-in" in rent_roll_df.columns:
        rent_roll_df["Move-in"] = pd.to_datetime(rent_roll_df["Move-in"], errors='coerce')
        today = pd.to_datetime("today")
        rent_roll_df["Tenure_Months"] = ((today - rent_roll_df["Move-in"]) / pd.Timedelta(days=30)).round()
    else:
        rent_roll_df["Tenure_Months"] = None

    rent_roll_df["Late Count"] = pd.to_numeric(rent_roll_df["Late Count"], errors="coerce")
    rent_roll_df["Late_Payment_Rate"] = (
        rent_roll_df["Late Count"] / rent_roll_df["Tenure_Months"]
    ).fillna(0)

    merged_df = tenant_directory_df.merge(
        rent_roll_df,
        on=["Property", "Unit", "Tenant"],
        how="inner",
        suffixes=("", "_rentroll")
    )

    final_merged_df = merged_df.merge(
        delinquency_df,
        on=["Property", "Unit", "Tenant"],
        how="left"
    )

    # Filter columns to only those needed
    final_merged_df = final_merged_df[[col for col in columns_needed if col in final_merged_df.columns]]

    output_csv = os.path.join(MERGED_FOLDER, "merged_tenant_data.csv")
    final_merged_df.to_csv(output_csv, index=False)
    return final_merged_df, output_csv

def save_insights_to_db(insights):
    with sqlite3.connect(DB_FILE) as conn:
        for entry in insights:
            conn.execute("""
                INSERT INTO tenant_insights (timestamp, tenant_name, property, insight_json)
                VALUES (?, ?, ?, ?)
            """, (
                datetime.now().isoformat(),
                entry.get("tenant_name"),
                entry.get("property"),
                json.dumps(entry)
            ))

# ---- ROUTES ----

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/merge", methods=["POST"])
def merge():
    files = request.files
    required = ["delinquency", "rent_roll", "tenant_directory"]
    if not all(name in files for name in required):
        return "Missing required files", 400

    paths = {}
    for name in required:
        f = files[name]
        if not allowed_file(f.filename):
            return f"Invalid file format: {f.filename}", 400
        path = os.path.join(UPLOAD_FOLDER, secure_filename(f.filename))
        f.save(path)
        paths[name] = path

    df, csv_path = merge_datasets(paths["delinquency"], paths["rent_roll"], paths["tenant_directory"])

    preview_html = df.head(5).to_html(classes="preview-table", index=False)  # ✅ preview only 5
    full_json = df.to_json(orient="records")  # ✅ send all for GPT filtering

    return jsonify({
        "preview": preview_html,
        "data": json.loads(full_json)
    })

@app.route("/generate-insights", methods=["POST"])
def generate_insights():
    all_data = request.get_json()
    if not all_data or not isinstance(all_data, list):
        return jsonify({"error": "Invalid input"}), 400

    # ✅ Use safe_float to filter delinquent tenants
    delinquent_tenants = [
        t for t in all_data
        if safe_float(t.get("Past Due", 0)) > 0 or safe_float(t.get("Delinquent Rent", 0)) > 0
    ]

    print(f"✅ Total delinquent tenants found: {len(delinquent_tenants)}")

    if not delinquent_tenants:
        print("❌ No delinquent tenants found.")
        return jsonify({"error": "No delinquent tenants found."}), 400

    sample = random.sample(delinquent_tenants, min(len(delinquent_tenants), 5))
    print("📤 Sample sent to GPT (showing full JSON):")
    print(json.dumps(sample, indent=2))

    try:
        response = requests.post(
            LAMBDA_URL,
            headers={"Content-Type": "application/json"},
            json={"json": sample}
        )
        response.raise_for_status()

        insights = response.json()["body"]
        save_insights_to_db(insights)
        return jsonify(insights)

    except Exception as e:
        print(f"❌ Error occurred during GPT call: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/download-csv")
def download_csv():
    return send_file(os.path.join(MERGED_FOLDER, "merged_tenant_data.csv"), as_attachment=True)

@app.route("/insights-history")
def insights_history():
    with sqlite3.connect(DB_FILE) as conn:
        rows = conn.execute("SELECT tenant_name, property, insight_json FROM tenant_insights ORDER BY id DESC LIMIT 50").fetchall()
        history = [{"tenant_name": r[0], "property": r[1], **json.loads(r[2])} for r in rows]
    return jsonify(history)

if __name__ == "__main__":
    app.run(debug=True)
