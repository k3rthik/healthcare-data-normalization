from pyspark.sql import SparkSession
from data_processor import *
import shutil
import os
import glob


# FLATTEN spark temp output into clean .csv
def flatten_csv(temp_dir: str, final_path: str):
    # Remove old file if exists
    if os.path.exists(final_path):
        os.remove(final_path)

    part_file = glob.glob(os.path.join(temp_dir, "part-*.csv"))[0]

    # Moving to final .csv
    shutil.move(part_file, final_path)

    # Deleting the temp folder
    shutil.rmtree(temp_dir)


def main():
    spark = (
        SparkSession.builder.appName("HealthcareDataNormalization")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    # Loading flat legacy dataset
    df = spark.read.option("header", "true").csv(
        "/Users/karthikpachabatla/project-3/data/dataset_18/legacy_healthcare_data.csv"
    )

    output_base = "/Users/karthikpachabatla/project-3/output"

    # Process all tables
    tables = {
        "DimPatient": process_dim_patient(df),
        "DimInsurance": process_dim_insurance(df),
        "DimBilling": process_dim_billing(df),
        "DimProvider": process_dim_provider(df),
        "DimLocation": process_dim_location(df),
        "DimPrimaryDiagnosis": process_dim_primary_diagnosis(df),
        "DimSecondaryDiagnosis": process_dim_secondary_diagnosis(df),
        "DimTreatment": process_dim_treatment(df),
        "DimPrescription": process_dim_prescription(df),
        "DimLabOrder": process_dim_lab_order(df),
        "FactVisit": process_fact_visit(df),
    }

    for name, dataframe in tables.items():
        temp_path = f"{output_base}_temp/{name}"
        final_path = f"{output_base}/{name}.csv"
        dataframe.coalesce(1).write.option("header", "true").mode("overwrite").csv(
            temp_path
        )
        flatten_csv(temp_path, final_path)

    spark.stop()


if __name__ == "__main__":
    main()
