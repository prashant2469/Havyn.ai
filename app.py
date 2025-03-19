from flask import Flask, render_template, request
import pandas as pd
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {"csv"}

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_data(df):
    """Removes rows where all columns are NaN."""
    return df.dropna(how="all").reset_index(drop=True)

def remove_duplicate_columns(df):
    """Removes duplicate columns dynamically (_x, _y suffixes)."""
    column_mapping = {}
    for col in df.columns:
        base_col = col.rstrip("_xy")
        if base_col not in column_mapping:
            column_mapping[base_col] = col
    df_cleaned = df[list(column_mapping.values())]
    df_cleaned = df_cleaned.rename(columns={v: k for k, v in column_mapping.items()})
    return df_cleaned

def merge_datasets(delinquency_path, rent_roll_path, tenant_directory_path):
    """Merges the three datasets, cleans them, and saves the merged dataset to the Downloads folder."""
    
    # Load datasets
    delinquency_df = pd.read_csv(delinquency_path)
    rent_roll_df = pd.read_csv(rent_roll_path)
    tenant_directory_df = pd.read_csv(tenant_directory_path)

    # Clean data (remove empty separator rows)
    delinquency_df = clean_data(delinquency_df)
    rent_roll_df = clean_data(rent_roll_df)
    tenant_directory_df = clean_data(tenant_directory_df)

    # Merge datasets using "Property Name" and "Unit"
    merged_df = rent_roll_df.merge(tenant_directory_df, on=["Property Name", "Unit"], how="left")
    merged_df = merged_df.merge(delinquency_df, on=["Property Name", "Unit"], how="left")

    # Remove unnecessary separator rows
    columns_except_unit = [col for col in merged_df.columns if col != "Unit"]
    merged_df_cleaned = merged_df.dropna(subset=columns_except_unit, how="all").reset_index(drop=True)

    # Remove duplicate columns dynamically
    merged_df_cleaned = remove_duplicate_columns(merged_df_cleaned)

    # Get user's Downloads folder dynamically
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")

    # Save the final cleaned dataset in Downloads
    output_file = os.path.join(downloads_folder, "final_cleaned_merged_tenant_data.csv")
    merged_df_cleaned.to_csv(output_file, index=False)

    return output_file

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Ensure all files are uploaded
        if not all(key in request.files for key in ["delinquency", "rent_roll", "tenant_directory"]):
            return "Please upload all required files.", 400
        
        delinquency_file = request.files["delinquency"]
        rent_roll_file = request.files["rent_roll"]
        tenant_directory_file = request.files["tenant_directory"]

        if not all(allowed_file(f.filename) for f in [delinquency_file, rent_roll_file, tenant_directory_file]):
            return "Invalid file format. Please upload CSV files.", 400

        # Save uploaded files
        delinquency_path = os.path.join(UPLOAD_FOLDER, secure_filename(delinquency_file.filename))
        rent_roll_path = os.path.join(UPLOAD_FOLDER, secure_filename(rent_roll_file.filename))
        tenant_directory_path = os.path.join(UPLOAD_FOLDER, secure_filename(tenant_directory_file.filename))

        delinquency_file.save(delinquency_path)
        rent_roll_file.save(rent_roll_path)
        tenant_directory_file.save(tenant_directory_path)

        # Process files and generate merged dataset
        output_file = merge_datasets(delinquency_path, rent_roll_path, tenant_directory_path)

        return f"<h3>✅ Merged file saved to: {output_file} (Check your Downloads folder!)</h3>"

    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)
