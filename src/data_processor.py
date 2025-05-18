from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, max as spark_max, when, md5, concat_ws
from pyspark.sql import functions as F


def process_dim_patient(df: DataFrame) -> DataFrame:
    latest_visits = df.groupBy("patient_id").agg(
        spark_max("visit_datetime").alias("last_visit")
    )
    patients = df.select(
        "patient_id",
        "patient_first_name",
        "patient_last_name",
        "patient_date_of_birth",
        "patient_gender",
        "patient_address_line1",
        "patient_address_line2",
        "patient_city",
        "patient_state",
        "patient_zip",
        "patient_phone",
        "patient_email",
    ).dropDuplicates(["patient_id"])

    patients = patients.join(latest_visits, on="patient_id", how="left")
    patients = patients.withColumn(
        "patient_status",
        when(col("last_visit") < lit("2022-01-01"), "Inactive").otherwise("Active"),
    ).drop("last_visit")

    return patients


def process_dim_insurance(df: DataFrame) -> DataFrame:
    return df.select(
        "insurance_id",
        "patient_id",
        "insurance_payer_name",
        "insurance_policy_number",
        "insurance_group_number",
        "insurance_plan_type",
    ).dropDuplicates(["insurance_id"])


def process_dim_billing(df: DataFrame) -> DataFrame:
    return df.select(
        "billing_id",
        "insurance_id",
        "billing_total_charge",
        "billing_amount_paid",
        "billing_date",
        "billing_payment_status",
    ).dropDuplicates(["billing_id"])


def process_dim_provider(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "provider_id", md5(concat_ws("", "doctor_name", "doctor_department"))
        )
        .select("provider_id", "doctor_name", "doctor_title", "doctor_department")
        .dropDuplicates(["provider_id"])
    )


def process_dim_location(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("location_id", md5(concat_ws("", "clinic_name", "room_number")))
        .select("location_id", "clinic_name", "room_number")
        .dropDuplicates(["location_id"])
    )


def process_dim_primary_diagnosis(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "primary_diagnosis_id",
            md5(concat_ws("", "primary_diagnosis_code", "primary_diagnosis_desc")),
        )
        .select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )
        .dropDuplicates(["primary_diagnosis_id"])
    )


def process_dim_secondary_diagnosis(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "secondary_diagnosis_id",
            md5(concat_ws("", "secondary_diagnosis_code", "secondary_diagnosis_desc")),
        )
        .select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )
        .dropDuplicates(["secondary_diagnosis_id"])
    )


def process_dim_treatment(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "treatment_id", md5(concat_ws("", "treatment_code", "treatment_desc"))
        )
        .select("treatment_id", "treatment_code", "treatment_desc")
        .dropDuplicates(["treatment_id"])
    )


def process_dim_prescription(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "prescription_id",
            "prescription_drug_name",
            "prescription_dosage",
            "prescription_frequency",
            "prescription_duration_days",
        )
        .dropna(subset=["prescription_id"])
        .dropDuplicates(["prescription_id"])
    )


def process_dim_lab_order(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "lab_order_id",
            "lab_test_code",
            "lab_name",
            "lab_result_value",
            "lab_result_units",
            "lab_result_date",
        )
        .dropna(subset=["lab_order_id"])
        .dropDuplicates(["lab_order_id"])
    )


def process_fact_visit(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn(
            "provider_id", md5(concat_ws("", "doctor_name", "doctor_department"))
        )
        .withColumn("location_id", md5(concat_ws("", "clinic_name", "room_number")))
        .withColumn(
            "primary_diagnosis_id",
            md5(concat_ws("", "primary_diagnosis_code", "primary_diagnosis_desc")),
        )
        .withColumn(
            "secondary_diagnosis_id",
            md5(concat_ws("", "secondary_diagnosis_code", "secondary_diagnosis_desc")),
        )
        .withColumn(
            "treatment_id", md5(concat_ws("", "treatment_code", "treatment_desc"))
        )
    )

    return df.select(
        "visit_id",
        "patient_id",
        "insurance_id",
        "billing_id",
        "provider_id",
        "location_id",
        "primary_diagnosis_id",
        "secondary_diagnosis_id",
        "treatment_id",
        "prescription_id",
        "lab_order_id",
        "visit_type",
        "visit_datetime",
    )
