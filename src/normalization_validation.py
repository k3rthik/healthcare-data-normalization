import pandas as pd
import os

# ✅ Your exact output path from screenshot
OUTPUT_PATH = "/Users/karthikpachabatla/project-3/output"

# List of dimension tables with their primary keys
dimension_tables = {
    "DimPatient": "patient_id",
    "DimInsurance": "insurance_id",
    "DimBilling": "billing_id",
    "DimProvider": "provider_id",
    "DimLocation": "location_id",
    "DimPrimaryDiagnosis": "primary_diagnosis_id",
    "DimSecondaryDiagnosis": "secondary_diagnosis_id",
    "DimTreatment": "treatment_id",
    "DimPrescription": "prescription_id",
    "DimLabOrder": "lab_order_id",
}

# Load fact table
fact = pd.read_csv(os.path.join(OUTPUT_PATH, "FactVisit.csv"))

print("=== ✅ DIMENSION TABLE VALIDATION ===\n")
for table, pk in dimension_tables.items():
    path = os.path.join(OUTPUT_PATH, f"{table}.csv")
    df = pd.read_csv(path)

    if df[pk].isnull().any():
        print(f"❌ {table}: NULLs found in primary key column `{pk}`")
    elif df[pk].duplicated().any():
        print(f"❌ {table}: Duplicate primary key values in `{pk}`")
    else:
        print(f"✅ {table}: Primary key `{pk}` is unique and non-null")

print("\n=== 🔗 FOREIGN KEY VALIDATION IN FACTVISIT ===\n")
for table, pk in dimension_tables.items():
    if pk in fact.columns:
        dim_df = pd.read_csv(os.path.join(OUTPUT_PATH, f"{table}.csv"))
        missing_keys = fact[~fact[pk].isin(dim_df[pk])]
        if len(missing_keys) > 0:
            print(
                f"❌ Foreign key `{pk}` in FactVisit has {len(missing_keys)} missing references in `{table}`"
            )
        else:
            print(f"✅ Foreign key `{pk}` in FactVisit matches `{table}` correctly")

# Check nulls in visit_id
if fact["visit_id"].isnull().any():
    print("\n❌ FactVisit: `visit_id` has null values")
else:
    print("\n✅ FactVisit: `visit_id` is non-null")

print("\n🎉 Validation complete.")
