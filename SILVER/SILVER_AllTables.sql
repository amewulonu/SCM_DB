-- SILVER LAYER: Stores cleaned and standardized data
-- Task to create all tables in SILVER layer (Transformed, Cleaned)
CREATE TABLE silver if not exist IS 'Cleaned and Transformed data ready for analysis';

SELECT * FROM bronze.Product_v6; IS 'to view the table while you make transformations'

-- CREATE TABLE siver.Product_v6
CREATE TABLE silver.Product_v6 (
    partNumber TEXT PRIMARY KEY,
    productType TEXT,
    categoryCode TEXT,
    brandCode TEXT,
    familyCode TEXT,
    lineCode TEXT,
    status TEXT,
    amount NUMERIC,
    Currency TEXT,
    defaultQuantityUnits TEXT,
    name TEXT,
    description TEXT,
    plannerCode TEXT,
    sourceLink TEXT
);
INSERT INTO silver.product_v6  (partNumber, productType, categoryCode, brandCode, familyCode, lineCode, status, amount, Currency,
defaultQuantityUnits, name, description, plannerCode, sourceLink
)
SELECT
partNumber,
productType,
categoryCode,
COALESCE(brandCode,'Unknown'),
COALESCE(familyCode, 'Unkown'),
COALESCE(lineCode, 'Unknown'),
status,
value AS amount,
valueCurrency AS currency,
defaultQuantityUnits,
name,
description,
plannerCode,
sourceLink
FROM bronze.Product_v6;

-- To view your transformed tables
SELECT * FROM silver.Organization_v3;

-- CREATE TABLE siver.organization_v3
CREATE TABLE silver.Organization_v3 (
    organizationIdentifier TEXT PRIMARY KEY,
    orgType TEXT,
    locationIdentifier TEXT,
    name TEXT,
    sourceLink TEXT
);
INSERT INTO silver.Organization_v3 (organizationIdentifier, orgType, locationIdentifier, name, sourceLink
)
SELECT
organizationIdentifier, 
orgType, 
locationIdentifier, 
name, 
sourceLink
FROM bronze.Organization_v3;

-- CREATE TABLE siver.Location_v3
CREATE TABLE silver.Location_v3 (
   locationIdentifier TEXT PRIMARY KEY,
   locationType TEXT,
   locationName TEXT,
   address1 TEXT,
   city TEXT,
   postalCode TEXT,
   stateProvince TEXT,
   country TEXT,
   coordinates TEXT,
   includeInCorrelation TEXT,
   geo TEXT,
   sourceLink TEXT
);
INSERT INTO silver.Location_v3 (locationIdentifier, locationType, locationName, address1, city, postalCode, stateProvince, country, coordinates, includeInCorrelation, geo, sourceLink
)
SELECT 
   locationIdentifier,
   locationType,
   locationName,
   address1,
   city,
   postalCode,
   stateProvince,
   country,
   coordinates,
   includeInCorrelation,
   geo,
   sourceLink
FROM bronze.Location_v3;

-- CREATE TABLE siver.Inventory_v2
CREATE TABLE silver.Inventory_v2 (
    partNumber TEXT,
    locationIdentifier TEXT,
    inventoryType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    amount NUMERIC,
    Currency TEXT,
    reservationOrders INTEGER,
    daysOfSupply INTEGER,
    shelfLife INTEGER,
    reorderLevel INTEGER,
    expectedLeadTime INTEGER,
    quantityUpperThreshold INTEGER,
    quantityLowerThreshold INTEGER,
    daysOfSupplyUpperThreshold INTEGER,
    daysOfSupplyLowerThreshold INTEGER,
    plannerCode TEXT,
    velocityCode TEXT,
    inventoryParentType TEXT,
    class TEXT,
    segment TEXT
);
INSERT INTO silver.Inventory_v2 ( partNumber, locationIdentifier, inventoryType, quantity, quantityUnits, amount,
Currency, reservationOrders, daysOfSupply, shelfLife, reorderLevel, expectedLeadTime, quantityUpperThreshold, quantityLowerThreshold, daysOfSupplyUpperThreshold, daysOfSupplyLowerThreshold,
plannerCode, velocityCode, inventoryParentType, class, segment)

SELECT
    partNumber,
    locationIdentifier,
    inventoryType,
    quantity,
    quantityUnits,
    value AS amount,
    valueCurrency AS Currency,
    reservationOrders,
    daysOfSupply,
    shelfLife,
    reorderLevel,
    expectedLeadTime,
    quantityUpperThreshold,
    quantityLowerThreshold,
    daysOfSupplyUpperThreshold,
    daysOfSupplyLowerThreshold,
    plannerCode,
    velocityCode,
    inventoryParentType,
    class,
    segment
FROM bronze.Inventory_v2;

-- CREATE TABLE silver.SupplyPlan_v2 
CREATE TABLE silver.SupplyPlan_v2 (
    product_partNumber TEXT,
    location_locationIdentifier TEXT,
    startDate DATE,
    duration TEXT,
    planParentType TEXT,
    planType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    source TEXT,
    sourceLink TEXT
);
INSERT INTO silver.SupplyPlan_v2 (product_partNumber, location_locationIdentifier, startDate, duration, planParentType,
planType, quantity, quantityUnits, source, sourceLink
)
SELECT
    "product.partNumber" AS product_partNumber,
    "location.locationIdentifier" AS location_locationIdentifier,
    startDate,
    duration,
    planParentType,
    planType,
    quantity,
    quantityUnits,
    source,
    sourceLink
FROM bronze.SupplyPlan_v2;

-- CREATE TABLE silver.Order_v3
CREATE TABLE silver.Order_v3 (
    orderIdentifier TEXT PRIMARY KEY,
    orderType TEXT,
    vendorOrganizationIdentifier TEXT,
    buyerOrganizationIdentifier TEXT,
    shipFromLocationIdentifier TEXT,
    shipToLocationIdentifier TEXT,
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
	sourceLink TEXT
);
INSERT INTO silver.order_v3 (orderIdentifier, orderType,  vendorOrganizationIdentifier, buyerOrganizationIdentifier,  
shipFromLocationIdentifier, shipToLocationIdentifier, createdDate, requestedShipDate, requestedDeliveryDate, plannedShipDate, plannedDeliveryDate,
quantity, quantityUnits, totalValue, orderValueCurrency, lineCount,totalShippedQuantity, sourceLink
)
SELECT
   orderIdentifier,
   orderType,
   vendorOrganizationIdentifier,
   buyerOrganizationIdentifier,
   shipFromLocationIdentifier,
	CASE
	   WHEN shipToLocationIdentifier = 'LT-1' Then 'London Track 1'
	   WHEN shipToLocationIdentifier = 'LT-2' Then 'London Track 2'
	 ELSE 'Unknown'
	 END AS shipToLocationIdentifier,
    createdDate,
    requestedShipDate,
    requestedDeliveryDate,
    plannedShipDate,
    plannedDeliveryDate,
    quantity,
    quantityUnits,
    totalValue,
    orderValueCurrency,
    lineCount,
    COALESCE(totalShippedQuantity,0),
	sourceLink
FROM bronze.Order_v3

-- CREATED silver.Shipment_v4
CREATE TABLE silver.Shipment_v4 (
    shipmentIdentifier TEXT PRIMARY KEY,
    shipmentType TEXT,
    shipFromLocationIdentifier TEXT,
    shipToLocationIdentifier TEXT,
    vendorOrganizationIdentifier TEXT,
    buyerOrganizationIdentifier TEXT,
    carrierOrganizationIdentifier TEXT,
    dateCreated TIMESTAMP,
    requestedTimeOfArrival TIMESTAMP,
    committedTimeOfArrival TIMESTAMP,
    actualShipDate TIMESTAMP,
    estimatedTimeOfArrival TIMESTAMP,
    predictedTimeOfArrival TIMESTAMP,
    actualTimeOfArrival TIMESTAMP,
    currentLocationCoordinates TEXT,
    currentRegion TEXT,
    transportMode TEXT,
    parcelTrackingNumber TEXT,
    sourceLink TEXT
);
INSERT INTO silver.Shipment_v4(
    shipmentIdentifier, shipmentType, shipFromLocationIdentifier, shipToLocationIdentifier,
    vendorOrganizationIdentifier, buyerOrganizationIdentifier, carrierOrganizationIdentifier,
    dateCreated, requestedTimeOfArrival, committedTimeOfArrival, actualShipDate,
    estimatedTimeOfArrival, predictedTimeOfArrival, actualTimeOfArrival,
    currentLocationCoordinates, currentRegion, transportMode, parcelTrackingNumber, sourceLink
)
SELECT
    shipmentIdentifier,
    shipmentType,
    shipFromLocationIdentifier,
    shipToLocationIdentifier,
    vendorOrganizationIdentifier,
    buyerOrganizationIdentifier,
    carrierOrganizationIdentifier,
    dateCreated::timestamp,
    requestedTimeOfArrival::timestamp,
    committedTimeOfArrival::timestamp,
    actualShipDate::timestamp,
    estimatedTimeOfArrival::timestamp,
    COALESCE(predictedTimeOfArrival::timestamp, '1970-01-01 00:00:00'::timestamp),
    COALESCE(actualTimeOfArrival::timestamp, '1970-01-01 00:00:00'::timestamp),
    COALESCE(currentLocationCoordinates, 'Unknown'),
    COALESCE(currentRegion, 'Unknown'),
    transportMode,
    parcelTrackingNumber,
    sourceLink
FROM bronze.shipment_v4;

