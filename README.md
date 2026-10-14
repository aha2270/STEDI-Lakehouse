# STEDI Human Balance Analytics — AWS Lakehouse Solution

This project implements an end-to-end Lakehouse architecture on AWS using S3, AWS Glue (PySpark), and Amazon Athena. The system ingests, sanitizes, and curates semi-structured IoT sensor data and customer profile records to train a machine learning model capable of detecting balance loss and fall prevention in real time.

---

## Architecture Overview

The data architecture is structured into three progressive data lake zones:

1. **Landing Zone:** Raw, unmodified JSON streaming files ingested directly into S3 and cataloged in AWS Glue Data Catalog via external Athena tables.
2. **Trusted Zone:** Data sanitized to enforce privacy regulations (filtering customers who agreed to share data for research purposes) and removing orphaned sensor readings.
3. **Curated Zone:** Business-ready data stores tailored for downstream analytical reporting and machine learning feature stores correlating accelerometer motion with step trainer readings at identical timestamps.

---

## Pipeline Execution & Data Flow

```text
               [ customer_landing ] (956)
                       │
                       ▼  (Filter shareWithResearchAsOfDate IS NOT NULL)
               [ customer_trusted ] (482)
              /                  \
             /                    \
[ accelerometer_landing ]          [ step_trainer_landing ]
        (81,273)                          (28,680)
            │                                 │
            ▼ (Join on email)                 ▼ (Join on serialNumber)
[ accelerometer_trusted ]          [ step_trainer_trusted ]
        (40,981)                          (14,460)
            │         \                    /
            │          \                  /
            │           ▼                ▼
            │        [ machine_learning_curated ] (43,681)
            │        (Join on sensorreadingtime == timestamp)
            ▼
 [ customers_curated ] (482)
 (Consented customers with accelerometer data)