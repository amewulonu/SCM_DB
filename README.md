# SCM_DB: Supply chain management project (pgAdmin)

Overview
SupplyChainDB project analyzes supply chain data to improve planning, shipment tracking, and logistics. Importing CSV data, designing a star schema with fact and dimension tables, cleaning and transforming data, and loading into MySQL. Advanced SQL stored procedures, CTEs, subqueries, and window functions, optimization enhances performance and insights.

It encompasses full-cycle data engineering steps, from ingestion to optimisation, utilising MySQL. 
________________________________________
## Project Workflow
1. Data Ingestion
Tasks:
•	Source: Provided CSV files
•	Tools: MySQL Workbench, Medallion Architecture (Bronze → Silver → Gold)
•	Task: Load raw CSVs into BRONZE schema staging tables.
 
2. Data Cleaning & Transformation
Task:
•	Handle missing values (e.g., impute weight using average values).
•	Standardise and enrich location data.
•	Convert date/time formats.
•	Resolve data inconsistencies.
•	Create DimDate from relevant timestamps.
3. Data Modelling (Star Schema)
Tasks:
•	Create an ER diagram for the schema. (Hint: Shipment can serve as a fact table, potentially with a SupplyPlan fact table as well.) Consider dimensions like:
o	Location (from both files)
o	Product (from SupplyPlan_v2.csv)
o	Vendor/Buyer/Carrier (from Shipment_v4.csv)
o	Date (for various date fields)
•	Normalise the data into fact and dimension tables. Design the tables to minimise redundancy and ensure data integrity.
•	Use SQL to transform and load data from the raw tables into the star schema.
Example Star Schema Suggestion:
•	Fact Tables:
o	ShipmentFacts: shipmentIdentifier (PK), shipmentType, shipFromLocationKey (FK), shipToLocationKey (FK), vendorKey (FK), buyerKey (FK), carrierKey (FK), status, dateCreatedKey (FK),
requestedTimeOfArrivalKey (FK), committedTimeOfArrivalKey (FK), actualShipDateKey (FK), estimatedTimeOfArrivalKey (FK), revisedEstimatedTimeOfArrivalKey (FK), predictedTimeOfArrivalKey (FK),
 actualTimeOfArrivalKey (FK), lineCount, weight, weightUnits.
o	SupplyPlanFacts: productKey (FK), locationKey (FK), startDateKey (FK), duration, planParentType, planType, quantity, quantityUnits, planningCycle, source, sourceLink
•	Dimension Tables:
o	DimLocation: locationKey (PK), locationIdentifier, region (from Shipment), other relevant location details.
o	DimProduct: productKey (PK), partNumber.
o	DimOrganization: organizationKey (PK), organizationIdentifier.
o	DimDate: dateKey (PK), date, year, month, day, dayOfWeek.

4. Load Data into Star Schema
Task:
•	Load the transformed data into the tables of your star schema (Populate GOLD tables from SILVER layer using SQL transformation scripts).

5. Advanced SQL Queries
Tasks:
•	Using Stored Procedure: Calculate the total quantity of products planned per location for a given date range.
•	Common Table Expression (CTE): Find the top 5 vendors with the most shipments in a specific region.
•	Subquery: Identify shipments with weights above the average weight for their shipment type.
•	Window Function: Calculate the rolling average of shipment weight over time for a specific carrier.

6. MySQL Optimisation
 Tasks:
•	Indexing: Add indexes to foreign key columns in fact tables and primary key columns in dimension tables. Index columns used in frequently executed queries.
•	Partitioning: Consider partitioning the ShipmentFacts table by actualShipDateKey if you have a large volume of data and frequently query based on date ranges.
•	Query Optimisation: Use EXPLAIN or Query Store to analyse and optimise slow queries.
________________________________________
##  Key Design Decisions
🔸 Why This Schema?
The star schema improves query performance and simplifies reporting by separating facts (measurable events) from dimensions (contextual data).
🔸 Handling Slowly Changing Dimensions
Use Type 2 SCD for tracking historical changes (e.g., carrier renames), preserving records with effective dates.
🔸 SQL Techniques for Missing Data
•	COALESCE to handle NULLs
•	Derived values via averages or lookups from similar entries
🔸 Data Consistency Strategy
•	Use JOINs to cross-verify records
•	Standardise identifiers and formats
•	Deduplicate where necessary
________________________________________
Questions for the Capstone Project 
Q: What's the difference between a CTE and a subquery?
A: CTEs improve readability and reusability; subqueries are nested within a main query. Use CTEs when logic must be reused or layered.
Q: How do window functions enhance performance?
A: They avoid unnecessary GROUP BY operations and let you calculate aggregates without collapsing rows.
Q: Benefits of Table Partitioning?
A: Speeds up queries on date ranges and large datasets by scanning only relevant partitions.
Q: Troubleshooting slow queries?
A: Use EXPLAIN, check index usage, avoid SELECT *, and monitor for expensive joins or subqueries.
________________________________________
To do… Repository Structure
graphql
CopyEdit
📁 SupplyChainDB
├── 📂 BRONZE.sql   # All Raw Table CSVs
├── 📂 GOLD.sql     # All DDL/DML scripts
├── 📂 SILVER.sql   # All Transformed tables
├── 📂 diagram      # Query results & insights
└── 📄 README.md    # Project summary and key insights

## further questions:
## Questions for this Capstone Project

### Why did you choose a particular schema for this project? Explain your rationale for selecting the fact and dimension tables.
The star schema was chosen for this project because it provides a clear and efficient structure for analytical queries in a supply chain context. 
It separates measurable facts (like shipments and supply plans) from descriptive dimensions (like product, location, date, and organisations), 
which simplifies querying, improves performance, and aligns well with data warehousing best practices.

This simplifies getting answers to complex business questions such as:
Using this schema, we can answer strategic business questions such as:

1. Which regions experience the highest shipping delays?

2. What is the average lead time from vendor to buyer by product category?

3. How has the planned inventory volume changed over different planning cycles?

4. What is the on-time delivery performance of each carrier over the last 6 months?

### How would you handle changes in dimension tables over time (e.g., a carrier changes its name)? Discuss strategies for handling slowly changing dimensions.
To handle changes in dimension tables over time.
I would implement Slowly Changing Dimension (SCD) Type 2. This preserves historical data by creating a new record with the updated information while retaining the original record. This approach ensures accurate historical reporting. Additional fields, such as EffectiveDate, EndDate, and IsCurrent, help track changes over time and maintain data integrity for time-based analyses.


### What SQL techniques did you use to handle missing data?
In this project, I used several SQL techniques to handle missing data, ensuring data quality and consistency:

1. LTRIM(RTRIM(...)) - To remove unwanted leading and trailing spaces before checking for nulls or blanks.

2. NULLIF(..., '') - To convert empty strings to NULL for uniformity.

3. ISNULL(..., default_value) - To replace NULL with appropriate default values like 'UNKNOWN', 'N/A', or 0.00.

4. CAST(... AS VARCHAR(MAX)) - To ensure a consistent datatype for string fields before transformations.

These techniques collectively cleaned and standardised the data for loading into dimension and fact tables.

### How did you identify and handle inconsistencies between the two datasets?
To identify inconsistencies;
1.  I performed cross-checks using JOIN operations between shared fields (e.g., locationIdentifier, organisationIdentifier, partNumber) across datasets. 
2. I used LEFT JOIN with IS NULL filters to spot mismatches. 
3. Then, I cleaned the data by trimming spaces (LTRIM(RTRIM(...))), handling casing inconsistencies, standardising formats, and replacing blanks with NULLIF and ISNULL defaults. 
This ensured referential integrity between fact and dimension tables.
