-- GOLD LAYER: Stores modeled date for analysis — usually fact + dimension tables (Star Schema).
-- Task to create all Facts and Dimension tables(Star Schema)
-- Product Dimension
CREATE TABLE gold.dimProduct_v6 (
    productKey SERIAL PRIMARY KEY,
    partNumber TEXT UNIQUE,
    productType TEXT,
    categoryCode TEXT,
    brandCode TEXT,
    familyCode TEXT,
    lineCode TEXT,
    status TEXT,
    amount NUMERIC,
    currency TEXT,
    defaultQuantityUnits TEXT,
    name TEXT,
    description TEXT,
    plannerCode TEXT,
    sourceLink TEXT
);
INSERT INTO gold.dimProduct_v6 (
    partNumber, productType, categoryCode, brandCode, familyCode,
    lineCode, status, amount, currency, defaultQuantityUnits,
    name, description, plannerCode, sourceLink
)
SELECT
    partNumber, productType, categoryCode, brandCode, familyCode,
    lineCode, status, amount, currency, defaultQuantityUnits,
    name, description, plannerCode, sourceLink
FROM silver.Product_v6;

-- Organization Dimension
CREATE TABLE gold.dimOrganization_v3 (
    organizationKey SERIAL PRIMARY KEY,
    organizationIdentifier TEXT UNIQUE,
    orgType TEXT,
    locationIdentifier TEXT,
    name TEXT,
    sourceLink TEXT
);
INSERT INTO gold.dimOrganization_v3 (
    organizationIdentifier, orgType, locationIdentifier, name, sourceLink
)
SELECT
    organizationIdentifier, orgType, locationIdentifier, name, sourceLink
FROM silver.Organization_v3;

-- Location Dimension
CREATE TABLE gold.dimLocation_v3 (
    locationKey SERIAL PRIMARY KEY,
    locationIdentifier TEXT UNIQUE,
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
INSERT INTO gold.dimLocation_v3 (
    locationIdentifier, locationType, locationName, address1, city,
    postalCode, stateProvince, country, coordinates,
    includeInCorrelation, geo, sourceLink
)
SELECT
    locationIdentifier, locationType, locationName, address1, city,
    postalCode, stateProvince, country, coordinates,
    includeInCorrelation, geo, sourceLink
FROM silver.Location_v3;

-- Date Dimension IS (populate from all date fields in Orders and Inventory)
CREATE TABLE gold.dimDate (
    dateKey SERIAL PRIMARY KEY,
    fullDate DATE UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    dayOfWeek INT
);
INSERT INTO gold.dimDate (fullDate, year, quarter, month, day, dayOfWeek)
SELECT DISTINCT
    d::DATE,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    EXTRACT(DOW FROM d)
FROM (
    SELECT createdDate AS d FROM silver.Order_v3
    UNION SELECT requestedShipDate FROM silver.Order_v3
    UNION SELECT requestedDeliveryDate FROM silver.Order_v3
    UNION SELECT plannedShipDate FROM silver.Order_v3
    UNION SELECT plannedDeliveryDate FROM silver.Order_v3
) dates
WHERE d IS NOT NULL;

CREATE TABLE gold.dimSupplyPlan_v2 (
    supplyPlanKey SERIAL PRIMARY KEY,
    productKey INT NOT NULL,
    locationKey INT NOT NULL,
    startDateKey INT,
    duration TEXT,
    planParentType TEXT,
    planType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    source TEXT,
    sourceLink TEXT,
    FOREIGN KEY (productKey) REFERENCES gold.dimProduct_v6(productKey),
    FOREIGN KEY (locationKey) REFERENCES gold.dimLocation_v3(locationKey),
    FOREIGN KEY (startDateKey) REFERENCES gold.dimDate(dateKey)
);
INSERT INTO gold.dimSupplyPlan_v2 (
    productkey, locationkey, startDatekey,
    duration, planParentType, planType,
    quantity, quantityUnits, source, sourceLink
)
SELECT
    dp.productkey,                -- from dimProduct
    dl.locationkey,               -- from dimLocation
    dd.datekey,                   -- from dimDate
    s.duration,
    s.planparenttype,
    s.plantype,
    s.quantity,
    s.quantityunits,
    s.source,
    s.sourcelink
FROM silver.supplyplan_v2 s
LEFT JOIN gold.dimproduct_v6 dp ON s.product_partnumber = dp.partnumber
LEFT JOIN gold.dimlocation_v3 dl ON s.location_locationIdentifier = dl.locationidentifier
LEFT JOIN gold.dimdate dd  ON s.startdate::DATE = dd.fulldate;

-- DIM Shipment
CREATE TABLE gold.dimShipment_v4 (
    shipmentKey SERIAL PRIMARY KEY,
    shipmentIdentifier TEXT UNIQUE NOT NULL,
    shipmentType TEXT,
    vendorOrgKey INT,
    buyerOrgKey INT,
    carrierOrgKey INT,
    shipFromLocKey INT,
    shipToLocKey INT,
    dateCreatedKey INT,
    requestedArrivalKey INT,
    committedArrivalKey INT,
    actualShipDateKey INT,
    estimatedArrivalKey INT,
    predictedArrivalKey INT,
    actualArrivalKey INT,
    currentLocationCoordinates TEXT,
    currentRegion TEXT,
    transportMode TEXT,
    parcelTrackingNumber TEXT,
    sourceLink TEXT,
    FOREIGN KEY (vendorOrgKey)      REFERENCES gold.dimOrganization_v3(organizationKey),
    FOREIGN KEY (buyerOrgKey)       REFERENCES gold.dimOrganization_v3(organizationKey),
    FOREIGN KEY (carrierOrgKey)     REFERENCES gold.dimOrganization_v3(organizationKey),
    FOREIGN KEY (shipFromLocKey)    REFERENCES gold.dimLocation_v3(locationKey),
    FOREIGN KEY (shipToLocKey)      REFERENCES gold.dimLocation_v3(locationKey),
    FOREIGN KEY (dateCreatedKey)        REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (requestedArrivalKey)   REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (committedArrivalKey)   REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (actualShipDateKey)     REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (estimatedArrivalKey)   REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (predictedArrivalKey)   REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (actualArrivalKey)      REFERENCES gold.dimDate(dateKey)
);
INSERT INTO gold.dimShipment_v4 (
    shipmentIdentifier, shipmentType,
    vendorOrgKey, buyerOrgKey, carrierOrgKey,
    shipFromLocKey, shipToLocKey,
    dateCreatedKey, requestedArrivalKey, committedArrivalKey,
    actualShipDateKey, estimatedArrivalKey, predictedArrivalKey, actualArrivalKey,
    currentLocationCoordinates, currentRegion, transportMode, parcelTrackingNumber, sourceLink
)
SELECT
    s.shipmentIdentifier,
    s.shipmentType,
    vo.organizationKey,
    bo.organizationKey,
    co.organizationKey,
    lsf.locationKey,
    lst.locationKey,
    dc.dateKey,
    rta.dateKey,
    cta.dateKey,
    asd.dateKey,
    eta.dateKey,
    pta.dateKey,
    ata.dateKey,
    s.currentLocationCoordinates,
    s.currentRegion,
    s.transportMode,
    s.parcelTrackingNumber,
    s.sourceLink
FROM silver.Shipment_v4 s
LEFT JOIN gold.dimOrganization_v3 vo ON s.vendorOrganizationIdentifier  = vo.organizationIdentifier
LEFT JOIN gold.dimOrganization_v3 bo ON s.buyerOrganizationIdentifier   = bo.organizationIdentifier
LEFT JOIN gold.dimOrganization_v3 co ON s.carrierOrganizationIdentifier = co.organizationIdentifier
LEFT JOIN gold.dimLocation_v3 lsf     ON s.shipFromLocationIdentifier   = lsf.locationIdentifier
LEFT JOIN gold.dimLocation_v3 lst     ON s.shipToLocationIdentifier     = lst.locationIdentifier
LEFT JOIN gold.dimDate dc   ON s.dateCreated::date             = dc.fullDate
LEFT JOIN gold.dimDate rta  ON s.requestedTimeOfArrival::date  = rta.fullDate
LEFT JOIN gold.dimDate cta  ON s.committedTimeOfArrival::date  = cta.fullDate
LEFT JOIN gold.dimDate asd  ON s.actualShipDate::date          = asd.fullDate
LEFT JOIN gold.dimDate eta  ON s.estimatedTimeOfArrival::date  = eta.fullDate
LEFT JOIN gold.dimDate pta  ON s.predictedTimeOfArrival::date  = pta.fullDate
LEFT JOIN gold.dimDate ata  ON s.actualTimeOfArrival::date     = ata.fullDate;

-- Orders Fact
CREATE TABLE gold.factOrder_v3 (
    orderKey SERIAL PRIMARY KEY,
    orderIdentifier TEXT UNIQUE,
    orderType TEXT,
    vendorOrgKey INT,
    buyerOrgKey INT,
    shipFromLocKey INT,
    shipToLocKey INT,
    createdDateKey INT,
    requestedShipDateKey INT,
    requestedDeliveryDateKey INT,
    plannedShipDateKey INT,
    plannedDeliveryDateKey INT,
    quantity INTEGER,
    quantityUnits TEXT,
    totalValue NUMERIC,
    orderValueCurrency TEXT,
    lineCount INTEGER,
    totalShippedQuantity INTEGER,
    FOREIGN KEY (vendorOrgKey) REFERENCES gold.dimOrganization_v3(organizationKey),
    FOREIGN KEY (buyerOrgKey) REFERENCES gold.dimOrganization_v3(organizationKey),
    FOREIGN KEY (shipFromLocKey) REFERENCES gold.dimLocation_v3(locationKey),
    FOREIGN KEY (shipToLocKey) REFERENCES gold.dimLocation_v3(locationKey),
    FOREIGN KEY (createdDateKey) REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (requestedShipDateKey) REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (requestedDeliveryDateKey) REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (plannedShipDateKey) REFERENCES gold.dimDate(dateKey),
    FOREIGN KEY (plannedDeliveryDateKey) REFERENCES gold.dimDate(dateKey)
);
INSERT INTO gold.factOrder_v3 (
    orderIdentifier, orderType, vendorOrgKey, buyerOrgKey,
    shipFromLocKey, shipToLocKey, createdDateKey,
    requestedShipDateKey, requestedDeliveryDateKey,
    plannedShipDateKey, plannedDeliveryDateKey, quantity,
    quantityUnits, totalValue, orderValueCurrency, lineCount, totalShippedQuantity
)
SELECT
    o.orderIdentifier,
    o.orderType,
    doVendor.organizationKey,
    doBuyer.organizationKey,
    dlFrom.locationKey,
    dlTo.locationKey,
    ddCreated.dateKey,
    ddReqShip.dateKey,
    ddReqDel.dateKey,
    ddPlanShip.dateKey,
    ddPlanDel.dateKey,
    o.quantity,
    o.quantityUnits,
    o.totalValue,
    o.orderValueCurrency,
    o.lineCount,
    o.totalShippedQuantity
FROM silver.Order_v3 o
LEFT JOIN gold.dimOrganization_v3 doVendor ON o.vendorOrganizationIdentifier = doVendor.organizationIdentifier
LEFT JOIN gold.dimOrganization_v3 doBuyer  ON o.buyerOrganizationIdentifier = doBuyer.organizationIdentifier
LEFT JOIN gold.dimLocation_v3 dlFrom       ON o.shipFromLocationIdentifier = dlFrom.locationIdentifier
LEFT JOIN gold.dimLocation_v3 dlTo         ON o.shipToLocationIdentifier = dlTo.locationIdentifier
LEFT JOIN gold.dimDate ddCreated        ON o.createdDate::DATE = ddCreated.fullDate
LEFT JOIN gold.dimDate ddReqShip        ON o.requestedShipDate::DATE = ddReqShip.fullDate
LEFT JOIN gold.dimDate ddReqDel         ON o.requestedDeliveryDate::DATE = ddReqDel.fullDate
LEFT JOIN gold.dimDate ddPlanShip       ON o.plannedShipDate::DATE = ddPlanShip.fullDate
LEFT JOIN gold.dimDate ddPlanDel        ON o.plannedDeliveryDate::DATE = ddPlanDel.fullDate;

-- Inventory Fact
CREATE TABLE gold.factInventory_v2 (
    inventoryKey SERIAL PRIMARY KEY,
    productKey INT,
    locationKey INT,
    inventoryType TEXT,
    quantity INTEGER,
    quantityUnits TEXT,
    amount NUMERIC,
    currency TEXT,
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
    segment TEXT,
    FOREIGN KEY (productKey) REFERENCES gold.dimProduct_v6(productKey),
    FOREIGN KEY (locationKey) REFERENCES gold.dimLocation_v3(locationKey)
);
INSERT INTO gold.factInventory_v2 (
    productKey, locationKey, inventoryType, quantity, quantityUnits, amount,
    currency, reservationOrders, daysOfSupply, shelfLife, reorderLevel,
    expectedLeadTime, quantityUpperThreshold, quantityLowerThreshold,
    daysOfSupplyUpperThreshold, daysOfSupplyLowerThreshold, plannerCode,
    velocityCode, inventoryParentType, class, segment
)
SELECT
    dp.productKey,
    dl.locationKey,
    inv.inventoryType,
    inv.quantity,
    inv.quantityUnits,
    inv.amount,
    inv.currency,
    inv.reservationOrders,
    inv.daysOfSupply,
    inv.shelfLife,
    inv.reorderLevel,
    inv.expectedLeadTime,
    inv.quantityUpperThreshold,
    inv.quantityLowerThreshold,
    inv.daysOfSupplyUpperThreshold,
    inv.daysOfSupplyLowerThreshold,
    inv.plannerCode,
    inv.velocityCode,
    inv.inventoryParentType,
    inv.class,
    inv.segment
FROM silver.Inventory_v2 inv
LEFT JOIN gold.dimProduct_v6 dp ON inv.partNumber = dp.partNumber
LEFT JOIN gold.dimLocation_v3 dl ON inv.locationIdentifier = dl.locationIdentifier;



-- Advanced SQL Queries: Using stored procedures, CTEs, subqueries, and window functions to analyze supply chain metrics.

SELECT * FROM gold.factInventory_v2;

-- 1. Stored Procedure: Total Quantity of Products Planned per Location for a Given Date Range
DROP PROCEDURE IF EXISTS gold.get_total_planned_quantity;

CREATE OR REPLACE FUNCTION gold.get_total_planned_quantity(start_date DATE, end_date DATE)
RETURNS TABLE (
    locationName TEXT,
    total_quantity BIGINT,
    quantityUnits TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT l.locationName,
           SUM(sp.quantity) AS total_quantity,
           sp.quantityUnits
    FROM gold.dimSupplyPlan_v2 sp
    JOIN gold.dimLocation_v3 l ON sp.locationKey = l.locationKey
    JOIN gold.dimDate d ON sp.startDateKey = d.dateKey
    WHERE d.fullDate BETWEEN start_date AND end_date
    GROUP BY l.locationName, sp.quantityUnits
    ORDER BY total_quantity DESC;
END;
$$;


-- 🔹 Example execution:
SELECT * 
FROM gold.get_total_planned_quantity('2025-01-01', '2025-01-31');


-- 2. CTE: Top 5 Vendors with the Most Shipments in a Specific Region

Here, we’ll use shipment quantities and filter by currentRegion from dimShipment_v4.
WITH vendor_shipments AS (
    SELECT 
        o.name AS vendor_name,
        COUNT(s.shipmentKey) AS shipment_count,
        SUM(sq.quantity) AS total_quantity
    FROM gold.dimShipment_v4 s
    JOIN gold.dimOrganization_v3 o ON s.vendorOrgKey = o.organizationKey
    LEFT JOIN gold.factOrder_v3 sq ON s.shipmentIdentifier = sq.orderIdentifier
    WHERE s.currentRegion = 'Europe'   -- Change region as needed
    GROUP BY o.name
)
SELECT vendor_name, shipment_count, total_quantity
FROM vendor_shipments
ORDER BY shipment_count DESC
LIMIT 5;

-- Subquery: Shipments with Quantity Above the Average per Shipment Type
SELECT 
    s.shipmentIdentifier, 
    s.shipmentType, 
    f.quantity, 
    f.quantityUnits
FROM gold.dimShipment_v4 s
JOIN gold.factOrder_v3 f ON s.shipmentIdentifier = f.orderIdentifier
WHERE f.quantity > (
    SELECT AVG(f2.quantity)
    FROM gold.dimShipment_v4 s2
    JOIN gold.factOrder_v3 f2 ON s2.shipmentIdentifier = f2.orderIdentifier
    WHERE s2.shipmentType = s.shipmentType
);

-- Subquery: Shipments with Quantity Above the Average per Shipment Type
SELECT 
    s.shipmentIdentifier, 
    s.shipmentType, 
    f.quantity, 
    f.quantityUnits
FROM gold.dimShipment_v4 s
JOIN gold.factOrder_v3 f ON s.shipmentIdentifier = f.orderIdentifier
WHERE f.quantity > (
    SELECT AVG(f2.quantity)
    FROM gold.dimShipment_v4 s2
    JOIN gold.factOrder_v3 f2 ON s2.shipmentIdentifier = f2.orderIdentifier
    WHERE s2.shipmentType = s.shipmentType
);

-- 4. Window Function: Rolling Average of Shipment Quantity Over Time for a Specific Carrier
SELECT 
    s.carrierOrgKey,
    d.fullDate,
    f.quantity,
    AVG(f.quantity) OVER (
        PARTITION BY s.carrierOrgKey
        ORDER BY d.fullDate
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_quantity
FROM gold.dimShipment_v4 s
JOIN gold.factOrder_v3 f ON s.shipmentIdentifier = f.orderIdentifier
JOIN gold.dimDate d ON s.dateCreatedKey = d.dateKey
WHERE s.carrierOrgKey = 101;   -- Replace with actual carrierOrgKey

