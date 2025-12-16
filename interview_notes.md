# Interview Notes
Used Azure Free Account with cost controls enabled to avoid unnecessary cloud spend.
I used a dedicated resource group and region-aligned Databricks workspace following Azure best practices.
Clusters were deployed without public IPs using secure cluster connectivity to reduce exposure.
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

