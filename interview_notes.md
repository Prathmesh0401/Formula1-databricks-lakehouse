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

Phase 6 – Silver Layer Transformation

• Designed Silver layer as a trusted contract layer to standardize and cleanse raw Bronze data
• Applied explicit schemas, data type normalization, and column standardization across all datasets
• Built conformed dimension tables (circuits, races, drivers, constructors) to support analytics
• Prepared clean, deduplicated fact data from race results for downstream processing
• Created a consolidated race_results dataset by joining facts and dimensions to reduce repeated joins
• Enforced data quality by removing duplicates and retaining only complete, valid records
• Preserved detailed, non-aggregated data in Silver while deferring business aggregations to Gold
