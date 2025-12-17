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
