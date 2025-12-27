# Interview Notes
Phase 1 – Environment Setup:
- Created Azure Databricks workspace with secure cluster connectivity (no public IP)
- Used Standard tier for development
- Configured resource group and region alignment
- Planned governance and encryption enhancements for later stages

Phase 2 – Databricks Fundamentals

- Validated Spark session and runtime
- Used magic commands (%fs, %sql)
- Worked with dbutils for file system operations
- Understood DBFS vs ADLS differences
- Used widgets for notebook parameterization


Phase 3 – ADLS Security

- Implemented OAuth-based authentication between Azure Databricks and ADLS Gen2
- Used Azure AD Service Principal with RBAC (Storage Blob Data Contributor)
- Managed credentials using Databricks-backed secret scopes (Standard tier)
- Avoided storage account keys and hardcoded secrets
- Validated read and write access from Databricks

Phase 4 – DBFS vs ADLS Mounting Strategy

• Understood the role of DBFS as Databricks-managed storage and its limitations for enterprise data lakes  
• Compared DBFS and ADLS Gen2 for scalability, security, and governance  
• Chose direct ADLS access using ABFSS paths instead of DBFS mounts  
• Avoided ADLS mounting to prevent credential persistence and security risks  
• Used dbutils.fs and Spark APIs to interact directly with ADLS containers  
• Followed best practices by separating raw, processed, and presentation data at the storage level  

Phase 5 – Formula1 Data Ingestion (Bronze Layer)

• Designed and implemented a production-grade Bronze layer using Azure Databricks and PySpark  
• Ingested raw Formula1 datasets from ADLS Gen2 using OAuth-based Service Principal authentication  
• Managed secrets securely using Databricks-backed secret scopes  
• Enforced explicit schemas for all datasets to avoid schema inference issues  
• Handled multiple data formats:
  – CSV files (circuits, races, lap_times)  
  – JSON files (constructors, results, qualifying)  
  – Nested JSON structures (drivers)  
  – Array-based JSON using explode pattern (pit_stops)  
• Implemented partitioning strategy for event data (races partitioned by race_year)  
• Processed large and multi-file datasets efficiently using Spark’s distributed file reading  
• Added ingestion audit columns (ingestion_date) for data lineage and traceability  
• Debugged and resolved Spark ABFS authentication issues caused by implicit access-key configurations  
• Followed enterprise data lake best practices:
  – No DBFS for raw data  
  – No storage account keys  
  – No mounts  
  – Direct ABFSS access to ADLS Gen2  


Phase 6 – Silver Layer Transformation

• Designed Silver layer as a trusted contract layer to standardize and cleanse raw Bronze data
• Applied explicit schemas, data type normalization, and column standardization across all datasets
• Built conformed dimension tables (circuits, races, drivers, constructors) to support analytics
• Prepared clean, deduplicated fact data from race results for downstream processing
• Created a consolidated race_results dataset by joining facts and dimensions to reduce repeated joins
• Enforced data quality by removing duplicates and retaining only complete, valid records
• Preserved detailed, non-aggregated data in Silver while deferring business aggregations to Gold

Phase 7 – Gold Layer Analytics

• Designed Gold datasets with business-ready metrics for BI consumption  
• Built calculated_race_results to centralize business logic (wins, podiums)  
• Created ranked driver_standing and constructor_standing using window functions  
• Used partitioned Delta tables for scalability and performance  
• Designed pipelines to be incremental, idempotent, and automation-ready  
• Optimized analytics by shifting heavy computations out of BI tools  


Phase 8 – Delta Migration (Gold Layer)

• Safely converted Hive-partitioned Gold Parquet datasets to Delta using CONVERT TO DELTA with explicit partition schema  
• Preserved existing data and partitioning during migration  
• Enabled ACID transactions, time travel, and MERGE-based updates on Gold tables  
• Validated Delta conversion using DESCRIBE DETAIL and Delta reads  

Phase 9 – BI & Analytics Presentation

• Built an interactive Databricks SQL dashboard using Delta Gold tables  
• Designed season-wise analytics with consistent global filtering  
• Combined tables, bar charts, and trend visuals for layered insights  
• Prevented metric distortion by enforcing SQL-defined aggregation  
• Optimized trend charts by limiting series to top performers  
• Applied professional dashboard design principles for clarity and usability  

Phase 10 – Workflow Orchestration

• Built an end-to-end Databricks Workflow for Bronze → Silver → Gold pipelines  
• Used job-level parameters to enable incremental and repeatable executions  
• Enforced task dependencies to guarantee data correctness  
• Used job clusters with autoscaling and auto-termination for cost efficiency  
• Configured retries and failure handling for production resilience  
• Configured alerting on failed and success of jobs over email.


Phase 11 – Enterprise Orchestration & Alerting (ADF + Logic Apps)

• Designed an enterprise-grade orchestration layer using Azure Data Factory to control Databricks pipelines
• Implemented parameterized ADF pipelines to orchestrate Bronze → Silver → Gold Databricks notebooks
• Enforced strict success-based dependencies to prevent downstream execution on upstream failures
• Built centralized failure handling to capture and react to any notebook-level failure
• Implemented success notifications triggered only after full pipeline completion
• Integrated ADF with Azure Logic Apps via HTTP Web Activities for decoupled alerting
• Designed Logic App workflows to branch dynamically on SUCCESS vs FAILURE events
• Delivered automated email notifications with pipeline name, run ID, environment, file date, and error context
• Applied production-grade retry and timeout policies to balance resiliency and cost
• Followed separation of concerns:

ADF for orchestration

Databricks for transformation

Logic Apps for communication


Phase 12.3 – Security & Secrets Management

• Implemented centralized secrets management using Azure Key Vault  
• Secured Databricks access to ADLS using OAuth-based authentication  
• Eliminated secrets from code, pipelines, and Git repositories  
• Applied identity-based access using Managed Identity and RBAC  
• Enforced least-privilege permissions across ADF, Databricks, and ADLS  

Phase 13 – Platform Governance & Maturity (Standard Tier)

• Implemented enterprise-grade governance using hive_metastore due to Standard tier constraints  
• Enforced data access control at ADLS container level using RBAC and Service Principals  
• Secured secrets using Azure Key Vault and eliminated credentials from code  
• Used Delta Lake features (MERGE, history, time travel) to guarantee data correctness and recoverability  
• Maintained strict Bronze/Silver/Gold separation to simulate catalog-level governance  
• Documented data lineage explicitly to compensate for missing Unity Catalog lineage  
• Designed the platform to be Unity Catalog–ready with minimal future migration effort  

