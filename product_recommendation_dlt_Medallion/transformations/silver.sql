-- ===========================================================
-- SILVER LAYER
-- Cleans, deduplicates, snapshots Bronze data
-- Fully aligned with current faker schemas
-- ===========================================================

-- ===========================================================
-- 1) SILVER CUSTOMERS
-- Latest snapshot per CustomerID
-- ===========================================================
CREATE OR REFRESH LIVE TABLE silver_customers
COMMENT "Cleaned customer dimension (latest snapshot per customer)"
AS
WITH cleaned AS (
  SELECT
    CAST(CustomerID AS BIGINT)                AS CustomerID,
    TRIM(CustomerName)                        AS CustomerName,
    CAST(ContactNumber AS STRING)             AS ContactNumber,
    CAST(Age AS INT)                          AS Age,
    TRIM(Gender)                              AS Gender,
    TRIM(Location)                            AS Location,
    TRIM(SubscriptionStatus)                  AS SubscriptionStatus,
    TRIM(PaymentMethod)                       AS PaymentMethod,
    COALESCE(CAST(PreviousPurchases AS INT), 0) AS PreviousPurchases,
    TRIM(FrequencyOfPurchases)                AS FrequencyOfPurchases,
    TRIM(PreferredSeason)                     AS PreferredSeason,
    CAST(AvgReviewRating AS DOUBLE)            AS AvgReviewRating,
    current_timestamp()                       AS ingest_ts
  FROM LIVE.bronze_customers
  WHERE CustomerID IS NOT NULL
),
dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY CustomerID
             ORDER BY ingest_ts DESC
           ) rn
    FROM cleaned
  )
  WHERE rn = 1
)
SELECT * FROM dedup;


-- ===========================================================
-- 2) SILVER PRODUCTS
-- Latest snapshot per ProductID (handles updates correctly)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE silver_products
COMMENT "Latest product snapshot per ProductID (price & stock updates handled)"
AS
WITH cleaned AS (
  SELECT
    CAST(ProductID AS BIGINT)        AS ProductID,
    TRIM(ProductName)                AS ProductName,
    TRIM(Category)                   AS Category,
    TRIM(Brand)                      AS Brand,
    TRIM(AvailableColors)            AS AvailableColors,
    TRIM(AvailableSizes)             AS AvailableSizes,
    TRIM(AvailableStorage)           AS AvailableStorage,
    CAST(MRP AS DOUBLE)              AS MRP,
    CAST(Price AS DOUBLE)            AS Price,
    CAST(DiscountPercent AS DOUBLE)  AS DiscountPercent,
    CAST(Stock AS INT)               AS Stock,
    CAST(Rating AS DOUBLE)           AS Rating,
    CAST(ReviewsCount AS INT)        AS ReviewsCount,
    CAST(LastUpdated AS TIMESTAMP)   AS LastUpdated,
    current_timestamp()              AS ingest_ts
  FROM LIVE.bronze_products
  WHERE ProductID IS NOT NULL
),
dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY ProductID
             ORDER BY LastUpdated DESC, ingest_ts DESC
           ) rn
    FROM cleaned
  )
  WHERE rn = 1
)
SELECT * FROM dedup;


-- ===========================================================
-- 3) SILVER SALES
-- FBT-aware fact table with derived Season
-- ===========================================================
CREATE OR REFRESH LIVE TABLE silver_sales
COMMENT "Cleaned sales fact table (FBT-aware, season derived)"
AS
WITH cleaned AS (
  SELECT
    CAST(b.OrderID AS STRING)         AS OrderID,
    CAST(b.CustomerID AS BIGINT)      AS CustomerID,
    CAST(b.ProductID AS BIGINT)       AS ProductID,
    TRIM(b.Category)                  AS Category,
    TRIM(b.Brand)                     AS Brand,
    TRIM(b.InteractionType)           AS InteractionType,
    CAST(b.Quantity AS INT)           AS Quantity,
    CAST(b.PriceAtPurchase AS DOUBLE) AS PriceAtPurchase,

    -- ✅ DERIVE SEASON FROM EVENT TIME
    CASE
      WHEN month(CAST(b.EventTime AS TIMESTAMP)) IN (11, 12, 1) THEN 'Winter'
      WHEN month(CAST(b.EventTime AS TIMESTAMP)) IN (3, 4, 5)  THEN 'Summer'
      WHEN month(CAST(b.EventTime AS TIMESTAMP)) IN (6, 7, 8)  THEN 'Monsoon'
      ELSE 'Festive'
    END AS Season,

    CAST(b.EventTime AS TIMESTAMP)    AS EventTime,
    current_timestamp()               AS ingest_ts
  FROM LIVE.bronze_sales b
  WHERE b.CustomerID IS NOT NULL
    AND b.ProductID IS NOT NULL
    AND b.InteractionType IS NOT NULL
    AND b.EventTime IS NOT NULL
),

dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (
             PARTITION BY
               OrderID,
               ProductID,
               InteractionType,
               EventTime
             ORDER BY ingest_ts DESC
           ) rn
    FROM cleaned
  )
  WHERE rn = 1
)

SELECT
  d.OrderID,
  d.CustomerID,
  d.ProductID,
  p.ProductName,
  d.Category,
  d.Brand,
  d.InteractionType,
  d.Quantity,
  d.PriceAtPurchase,
  d.Season,
  d.EventTime,
  d.ingest_ts
FROM dedup d
LEFT JOIN LIVE.silver_products p
  ON d.ProductID = p.ProductID;
