# CSV to Postgres ETL Pipeline on Databricks

This project demonstrates how to upload and convert data from a CSV file into a PostgreSQL database using Databricks on AWS. The ETL (Extract, Transform, Load) process is designed to ensure data integrity through various verification techniques, such as row count checks, sampling, and checksum validation.

## Features
- **Extract**: Reads CSV data from AWS S3.
- **Transform**: Cleans and transforms the data using Apache Spark.
- **Load**: Writes the transformed data to a PostgreSQL database using JDBC.
- **Verification**: Validates data accuracy with row count comparison, data sampling, and checksum validation.

## Prerequisites

- AWS S3 bucket (to store the CSV files).
- Databricks workspace with Spark configured.
- PostgreSQL instance on AWS (e.g., RDS).
- AWS CLI or programmatic access to AWS services.
- JDBC driver for PostgreSQL.

## Setup

### 1. Environment Configuration

Ensure your environment is set up with the necessary dependencies:
- Databricks with access to AWS services (S3 and RDS).
- Access to a PostgreSQL database instance (username, password, and connection string).

### 2. Clone the Repository

Clone this repository to your local machine:

```bash
git clone https://github.com/<username>/<repository>.git
cd <repository>
```

### 3. CSV to Postgres ETL Script

The ETL script extracts data from a CSV file in AWS S3, transforms it using Apache Spark, and loads the data into a PostgreSQL database:

```python
# Extract: Read CSV from S3
df = spark.read.csv("s3://bucket/path/to/file.csv", header=True, inferSchema=True)

# Transform: Data cleansing and transformation
df_cleaned = df.na.drop()  # Drop rows with null values
df_transformed = df_cleaned.withColumn("new_col", df_cleaned["existing_col"] + 1)

# Load: Write the transformed data to PostgreSQL
jdbc_url = "jdbc:postgresql://<postgres-host>:5432/<database>"
db_properties = {
    "user": "username",
    "password": "password",
    "driver": "org.postgresql.Driver"
}
df_transformed.write.jdbc(url=jdbc_url, table="target_table", mode="append", properties=db_properties)
```

### 4. Data Verification

To ensure data accuracy during the ETL process, the script includes various verification steps:

- **Row Count Comparison**: Ensures the number of rows in the source CSV matches the target PostgreSQL table.
- **Data Sampling**: Compares a random sample of records from both the source and target.
- **Checksum Validation**: Calculates and compares checksums (MD5) for key columns to verify data integrity.

```python
# Row Count Comparison
source_row_count = df.count()
target_df = spark.read.jdbc(url=jdbc_url, table="target_table", properties=db_properties)
target_row_count = target_df.count()

if source_row_count == target_row_count:
    print("Row count matches")
else:
    print(f"Row count mismatch: {source_row_count} vs {target_row_count}")

# Data Sampling
source_sample = df.sample(fraction=0.1).collect()
target_sample = target_df.sample(fraction=0.1).collect()

for row in source_sample:
    if row not in target_sample:
        print(f"Mismatch found for row: {row}")

# Checksum Validation
from pyspark.sql.functions import md5, concat_ws

df_source_checksum = df.withColumn("checksum", md5(concat_ws(",", *df.columns)))
df_target_checksum = target_df.withColumn("checksum", md5(concat_ws(",", *target_df.columns)))

checksum_diff = df_source_checksum.subtract(df_target_checksum)
if checksum_diff.count() == 0:
    print("Data integrity verified: No differences found")
else:
    print("Data integrity issues detected")
```

## Running the ETL Process

1. **Configure AWS S3**: Ensure your CSV files are uploaded to an S3 bucket.

2. **Run the ETL**: Execute the ETL script in Databricks to extract, transform, and load the data from S3 to PostgreSQL.

3. **Verify Data**: Use the verification scripts to ensure that the data was correctly converted and uploaded to the PostgreSQL database.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Contributing

If you'd like to contribute to this project, feel free to fork the repository and submit a pull request.

```
