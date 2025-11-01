# SQL-Data-Warehouse  
> A sample data-warehouse project implementing the Medallion Architecture (Bronze → Silver → Gold layers)  

## 📄 Table of Contents  
- [Project Overview](#project-overview)  
- [Architecture – Medallion Layers](#architecture--medallion-layers)  


## Project Overview  
This project demonstrates the ingestion, transformation, and curation of data using a tiered “Medallion Architecture” approach. The goal is to move raw data through successive refinement stages (Bronze → Silver → Gold) to produce analytics-ready datasets and support insights, reporting, or downstream consumption.


## Architecture – Medallion Layers  
The Medallion Architecture is a layered design pattern used to **incrementally improve data quality** and readiness as data flows through each layer.

| Layer    | Description                                                |
|-----------|------------------------------------------------------------|
| Bronze    | Raw ingestion zone — minimal transformation, store as-is   |
| Silver    | Cleaned, & enriched layer                        |
| Gold      | Curated, analytics-ready / business-ready layer            |

In this project:  
- Data is first loaded into the **Bronze** stage (DDL scripts + raw data).  
- Then data is cleansed and structured in the **Silver** stage.  
- Finally, business-friendly tables/aggregations are produced in the **Gold** stage.  
This layered approach helps with traceability, auditability, reuse, and separation of concerns.

![Medallion Architecture](https://github.com/afrasamrin/SQL-Data-Warehouse/blob/main/ETL%20Data%20Architecture%20(1).jpg)

![Integration Model](https://github.com/afrasamrin/SQL-Data-Warehouse/blob/main/Data%20Flow.jpg)






