# Datawarehouse_Project
A Data Warehouse & Analytical project implemented in SQL Server, following the Medallion Architecture.
This project is implemented as a portfolio project with industry best practices in data engineering & Analytics. Here, I have built a data warehouse and generated actionable Insights

I have used the data source from @DataWithBaraa. Many thanks Baraa!!

## <img width="20" height="20" alt="image" src="https://github.com/user-attachments/assets/0e9b5c0a-b260-4950-bf3c-031b271af712" />Data Architecture
The data architecture follows Medallion Architecture-Bronze, Silver & Gold layer
<img width="861" height="584" alt="SQL_Datawarehouse_Project drawio" src="https://github.com/user-attachments/assets/126a980d-0c85-434d-8bb9-d1b4a3e5eb0c" />

-**Bronze Layer:** Stores the raw data, AS-IS from the source systems. Data is ingested from CSV files into SQL server Database.

-**Silver Layer:** Data cleansing and standardization will be done in this layer.

-**Gold Layer:** Business ready data will be presented in this layer for Reporting and Analytics.


