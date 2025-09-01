SELECT * FROM SILVER.Shipment_v4;

SELECT * FROM gold.factInventory_v2;

-- 1. Stored Procedure: Total Quantity of Products Planned per Location for a Given Date Range
CREATE OR REPLACE PROCEDURE gold.get_total_planned_quantity(
    start_date DATE,
    end_date DATE
)
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Total Planned Quantity Per Location between % and %:', start_date, end_date;

    -- Aggregated output
    PERFORM (
        SELECT l.locationName,
               SUM(sp.quantity) AS total_quantity,
               sp.quantityUnits
        FROM gold.dimSupplyPlan_v2 sp
        JOIN gold.dimLocation_v3 l ON sp.locationKey = l.locationKey
        JOIN gold.dimDate d ON sp.startDateKey = d.dateKey
        WHERE d.fullDate BETWEEN start_date AND end_date
        GROUP BY l.locationName, sp.quantityUnits
        ORDER BY total_quantity DESC
    );
END;
$$;

-- 🔹 Example execution:
CALL gold.get_total_planned_quantity('2025-01-01', '2025-01-31');

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
