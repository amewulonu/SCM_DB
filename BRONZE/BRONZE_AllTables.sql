-- Step 1: Created database SCM_DB.
-- Connected to database to create SCM_DB
DATABASE scm_DB IS 'Supply Chain Management Database for ETL and analytics';


-- Step 2: Created 3 schemas using medalion architecture.
-- BRONZE SCHEMA: stores raw CSV data
CREATE SCHEMA bronze; IS 'Raw data imported from CSV files, no transformations applied'

-- SILVER schema: stores cleaned and standardized data
CREATE SCHEMA silver; IS 'Cleaned and transformed data, ready for analysis';

-- GOLD schema: stores aggregated, analytical-ready data
CREATE SCHEMA gold; IS 'Aggregated and summarized data for reporting and dashboards';


-- Step 3: Created 7 tables and bulk loaded them with the Raw CSV files.
-- Created table Product_v6
CREATE TABLE bronze.Product_v6 (
    partNumber TEXT PRIMARY KEY,
    productType TEXT,
    categoryCode TEXT,
    brandCode TEXT,
    familyCode TEXT,
    lineCode TEXT,
    productSegmentCode TEXT,
    status TEXT,
    value NUMERIC,
    valueCurrency TEXT,
    defaultQuantityUnits TEXT,
    name TEXT,
    description TEXT,
    plannerCode TEXT,
    sourceLink TEXT
);
-- Bulk inserted raw csv file Product_v6
COPY bronze.Product_v6
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Product_v6.csv'
DELIMITER ','
CSV HEADER

-- Created table Organization_v3
CREATE TABLE bronze.Organization_v3 (
    organizationIdentifier TEXT PRIMARY KEY,
    orgType TEXT,
    locationIdentifier TEXT,
    name TEXT,
    division TEXT,
    sourceLink TEXT
);
-- Bulk inserted raw csv file Organization_v3
COPY bronze.Organization_v3
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Organization_v3.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Created table Location_v3
CREATE TABLE bronze.Location_v3 (
   locationIdentifier TEXT PRIMARY KEY,
   locationType TEXT,
   locationName TEXT,
   address1 TEXT,
   address2 TEXT,
   city TEXT,
   postalCode TEXT,
   stateProvince TEXT,
   country TEXT,
   coordinates TEXT,
   includeInCorrelation TEXT,
   geo TEXT,
   sourceLink TEXT
);
-- Bulk inserted raw csv file Location_v3
COPY bronze.Location_v3
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Location_v3.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Created table Inventory_v2 
CREATE TABLE bronze.Inventory_v2 (
    partNumber TEXT,
    locationIdentifier TEXT,
    inventoryType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    value NUMERIC,
    valueCurrency TEXT,
    reservationOrders INTEGER,
    daysOfSupply INTEGER,
    shelfLife INTEGER,
    reorderLevel INTEGER,
    expectedLeadTime INTEGER,
    quantityUpperThreshold INTEGER,
    quantityLowerThreshold INTEGER,
    daysOfSupplyUpperThreshold INTEGER,
    daysOfSupplyLowerThreshold INTEGER,
    expiringThreshold INTEGER,
    plannerCode TEXT,
    velocityCode TEXT,
    inventoryParentType TEXT,
    class TEXT,
    segment TEXT
);
-- Bulk inserted raw csv file Inventory_v2
COPY bronze.Inventory_v2
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Inventory_v2.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Created table SupplyPlan_v2
CREATE TABLE bronze.SupplyPlan_v2 (
    "product.partNumber" TEXT,
    "location.locationIdentifier" TEXT,
    startDate DATE,
    duration TEXT,
    planParentType TEXT,
    planType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    planningCycle TEXT,
    source TEXT,
    sourceLink TEXT
);
-- Bulk inserted raw csv file SupplyPlan_v2
COPY bronze.SupplyPlan_v2
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/SupplyPlan_v2.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Created table Order_v3
CREATE TABLE bronze.Order_v3 (
    orderIdentifier TEXT PRIMARY KEY,
    orderType TEXT,
    vendorOrganizationIdentifier TEXT,
    buyerOrganizationIdentifier TEXT,
    shipFromLocationIdentifier TEXT,
    shipToLocationIdentifier TEXT,
    orderStatus TEXT,
    createdDate TIMESTAMP,
    requestedShipDate TIMESTAMP,
    requestedDeliveryDate TIMESTAMP,
    plannedShipDate TIMESTAMP,
    plannedDeliveryDate TIMESTAMP,
    quantity INTEGER,
    quantityUnits TEXT,
    totalValue NUMERIC,
    orderValueCurrency TEXT,
    lineCount INTEGER,
    totalShippedQuantity INTEGER,
    exclude TEXT,
    sourceLink TEXT
);
-- Bulk inserted raw Order_v3 CSV files
COPY bronze.Order_v3
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Order_v3.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Created table Shipment_v4.csv
CREATE TABLE bronze.Shipment_v4 (
    shipmentIdentifier TEXT PRIMARY KEY,
    shipmentType TEXT,
    shipFromLocationIdentifier TEXT,
    shipToLocationIdentifier TEXT,
    vendorOrganizationIdentifier TEXT,
    buyerOrganizationIdentifier TEXT,
    carrierOrganizationIdentifier TEXT,
    status TEXT,
    dateCreated TEXT,
    requestedTimeOfArrival TEXT,
    committedTimeOfArrival TEXT,
    actualShipDate TEXT,
    estimatedTimeOfArrival TEXT,
    revisedEstimatedTimeOfArrival TEXT,
    predictedTimeOfArrival TEXT,
    actualTimeOfArrival TEXT,
    lineCount INTEGER,
    weight REAL,
    weightUnits TEXT,
    currentLocationCoordinates TEXT,
    currentRegion TEXT,
    transportMode TEXT,
    houseAirwayBill TEXT,
    parcelTrackingNumber TEXT,
    airwayMasterNumber TEXT,
    billOfLadingNumber TEXT,
    proNumber TEXT,
    manifest TEXT,
    exclude TEXT,
    sourceLink TEXT
);
-- Bulk inserted raw Shipment_v4 CSV files 
COPY bronze.Shipment_v4
FROM 'C:/Data_Tech/Portfolio projects/Mentor_proj_SSME_SQL/proj2/Shipment_v4.csv'
DELIMITER ','
CSV HEADER
ENCODING 'WIN1252';

-- Review to ensure all CSV files were successfully loaded to the BRONZE layer
SELECT * FROM bronze.Product_v6;
