-- ===========================================================
-- GOLD LAYER (DLT)
-- Reads: LIVE.silver_customers, LIVE.silver_products, LIVE.silver_sales
-- Supports FBT, CF, content-based & hybrid recommendations
-- ===========================================================

-- ===========================================================
-- 0) CUSTOMER AGE GROUP DIMENSION
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_customers_with_age_group
COMMENT "Customer dimension with derived AgeGroup"
AS
SELECT
  CustomerID,
  CustomerName,
  Age,
  Gender,
  Location,
  SubscriptionStatus,
  FrequencyOfPurchases,
  PreferredSeason,
  AvgReviewRating,
  CASE
    WHEN Age IS NULL THEN 'Unknown'
    WHEN Age < 18 THEN 'Under 18'
    WHEN Age BETWEEN 18 AND 24 THEN '18-24'
    WHEN Age BETWEEN 25 AND 34 THEN '25-34'
    WHEN Age BETWEEN 35 AND 44 THEN '35-44'
    WHEN Age BETWEEN 45 AND 54 THEN '45-54'
    WHEN Age BETWEEN 55 AND 64 THEN '55-64'
    ELSE '65+'
  END AS AgeGroup
FROM LIVE.silver_customers
WHERE CustomerID IS NOT NULL;


-- ===========================================================
-- 1) ENRICHED SALES FACT TABLE (CORE GOLD FACT)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_sales_enriched
COMMENT "Sales fact enriched with customer & product attributes"
AS
SELECT
  s.OrderID,
  s.CustomerID,
  c.CustomerName,
  c.Age,
  c.Gender,
  c.Location              AS CustomerLocation,
  c.AgeGroup,
  s.ProductID,
  p.ProductName,
  p.Category,
  p.Brand,
  p.Price                 AS CurrentProductPrice,
  s.PriceAtPurchase,
  p.DiscountPercent,
  p.Stock,
  p.Rating                AS ProductRating,
  p.ReviewsCount,
  s.InteractionType,
  s.Quantity,
  s.Season,
  s.EventTime
FROM LIVE.silver_sales s
LEFT JOIN LIVE.gold_customers_with_age_group c
  ON s.CustomerID = c.CustomerID
LEFT JOIN LIVE.silver_products p
  ON s.ProductID = p.ProductID
WHERE s.CustomerID IS NOT NULL
  AND s.ProductID IS NOT NULL;


-- ===========================================================
-- 2) USER–PRODUCT INTERACTION MATRIX (CF INPUT)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_user_product_interactions
COMMENT "User-product interactions aggregated for CF models"
AS
SELECT
  CustomerID,
  ProductID,
  SUM(COALESCE(Quantity, 1)) AS interaction_score,
  COUNT(*)                  AS interaction_events,
  MAX(EventTime)            AS last_interaction_ts
FROM LIVE.gold_sales_enriched
WHERE lower(InteractionType) = 'purchase'
GROUP BY CustomerID, ProductID;


-- ===========================================================
-- 3) PRODUCT FEATURES (CONTENT-BASED)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_product_features
COMMENT "Product-level features for similarity & metadata-based recs"
AS
SELECT
  ProductID,
  MAX(ProductName)          AS ProductName,
  MAX(Category)             AS Category,
  MAX(Brand)                AS Brand,
  MAX(CurrentProductPrice)  AS CurrentProductPrice,
  MAX(DiscountPercent)      AS DiscountPercent,
  MAX(Stock)                AS Stock,
  MAX(ProductRating)        AS ProductRating,
  MAX(ReviewsCount)         AS ReviewsCount
FROM LIVE.gold_sales_enriched
GROUP BY ProductID;


-- ===========================================================
-- 4) FREQUENTLY BOUGHT TOGETHER (FBT)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_frequently_bought_together
COMMENT "Product pairs appearing together in the same purchase order"
AS
WITH order_items AS (
  SELECT DISTINCT OrderID, ProductID
  FROM LIVE.gold_sales_enriched
  WHERE lower(InteractionType) = 'purchase'
),
pairs AS (
  SELECT
    a.ProductID AS ProductID_A,
    b.ProductID AS ProductID_B,
    a.OrderID
  FROM order_items a
  JOIN order_items b
    ON a.OrderID = b.OrderID
   AND a.ProductID < b.ProductID
)
SELECT
  ProductID_A,
  ProductID_B,
  COUNT(DISTINCT OrderID) AS together_orders
FROM pairs
GROUP BY ProductID_A, ProductID_B
HAVING COUNT(DISTINCT OrderID) >= 1;


-- ===========================================================
-- 5) TRENDING PRODUCTS (LAST 30 DAYS)
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_trending_products_30d
COMMENT "Products trending over the last 30 days"
AS
SELECT
  ProductID,
  MAX(ProductName) AS ProductName,
  MAX(Category)    AS Category,
  MAX(Brand)       AS Brand,
  COUNT(*)         AS purchase_count,
  SUM(Quantity)    AS total_quantity,
  COUNT(DISTINCT CustomerID) AS unique_customers,
  MAX(EventTime)   AS last_purchase_ts
FROM LIVE.gold_sales_enriched
WHERE lower(InteractionType) = 'purchase'
  AND EventTime >= date_sub(current_date(), 30)
GROUP BY ProductID;


-- ===========================================================
-- 6) USER CATEGORY PREFERENCE PROFILE
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_user_category_preferences
COMMENT "User affinity towards product categories"
AS
SELECT
  CustomerID,
  Category,
  COUNT(*)      AS purchase_count,
  SUM(Quantity) AS total_quantity,
  MAX(EventTime) AS last_purchase_ts
FROM LIVE.gold_sales_enriched
WHERE lower(InteractionType) = 'purchase'
  AND Category IS NOT NULL
GROUP BY CustomerID, Category;


-- ===========================================================
-- 7) USER PRICE PROFILE
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_user_price_profile
COMMENT "User spending behavior profile"
AS
SELECT
  CustomerID,
  AVG(PriceAtPurchase)                 AS avg_price_paid,
  MIN(PriceAtPurchase)                 AS min_price_paid,
  MAX(PriceAtPurchase)                 AS max_price_paid,
  SUM(PriceAtPurchase * Quantity)      AS total_spent,
  COUNT(*)                             AS total_orders
FROM LIVE.gold_sales_enriched
WHERE lower(InteractionType) = 'purchase'
GROUP BY CustomerID;


-- ===========================================================
-- 8) PRODUCT PRICE BANDS
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_product_price_bands
COMMENT "Product price segmentation"
AS
SELECT
  ProductID,
  ProductName,
  Category,
  Brand,
  CurrentProductPrice,
  CASE
    WHEN CurrentProductPrice < 500 THEN 'Low'
    WHEN CurrentProductPrice BETWEEN 500 AND 2000 THEN 'Mid'
    WHEN CurrentProductPrice BETWEEN 2000 AND 5000 THEN 'High'
    ELSE 'Premium'
  END AS price_band
FROM LIVE.gold_product_features;


-- ===========================================================
-- 9) AGE-GROUP POPULAR PRODUCTS
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_agegroup_popular_products
COMMENT "Top products per age group"
AS
SELECT
  c.AgeGroup,
  s.ProductID,
  MAX(s.ProductName) AS ProductName,
  MAX(s.Category)    AS Category,
  COUNT(*)           AS purchase_count,
  SUM(Quantity)      AS total_quantity
FROM LIVE.gold_sales_enriched s
LEFT JOIN LIVE.gold_customers_with_age_group c
  ON s.CustomerID = c.CustomerID
WHERE lower(s.InteractionType) = 'purchase'
GROUP BY c.AgeGroup, s.ProductID;


-- ===========================================================
-- 10) RECENCY-WEIGHTED USER–PRODUCT INTERACTIONS
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_user_product_interactions_weighted
COMMENT "Recency-weighted user-product interaction scores"
AS
SELECT
  CustomerID,
  ProductID,
  SUM(
    CASE
      WHEN lower(InteractionType) = 'purchase'    THEN 5.0
      WHEN lower(InteractionType) = 'add_to_cart' THEN 3.0
      WHEN lower(InteractionType) = 'view'        THEN 1.0
      ELSE 0.5
    END
    *
    EXP(-0.1 * DATEDIFF(current_date(), EventTime))
  ) AS recency_weighted_score,
  COUNT(*)       AS interaction_events,
  MAX(EventTime) AS last_interaction_ts
FROM LIVE.gold_sales_enriched
GROUP BY CustomerID, ProductID;


-- ===========================================================
-- 11) HYBRID USER–PRODUCT SCORES
-- ===========================================================
CREATE OR REFRESH LIVE TABLE gold_hybrid_user_product_scores
COMMENT "Hybrid recommendation scores (CF + Recency + Content)"
AS
SELECT
  up.CustomerID,
  up.ProductID,

  up.interaction_score * 0.5                                AS cf_weighted_score,
  COALESCE(w.recency_weighted_score, 0) * 0.3               AS recency_weighted_score,

  CASE
    WHEN pf.Brand = sp.Brand     THEN 1.0
    WHEN pf.Category = sp.Category THEN 0.7
    ELSE 0.2
  END * 0.2                                                  AS content_similarity_score,

  (
    up.interaction_score * 0.5 +
    COALESCE(w.recency_weighted_score, 0) * 0.3 +
    CASE
      WHEN pf.Brand = sp.Brand     THEN 1.0
      WHEN pf.Category = sp.Category THEN 0.7
      ELSE 0.2
    END * 0.2
  ) AS hybrid_score

FROM LIVE.gold_user_product_interactions up
LEFT JOIN LIVE.gold_user_product_interactions_weighted w
  ON up.CustomerID = w.CustomerID
 AND up.ProductID = w.ProductID
LEFT JOIN LIVE.gold_product_features pf
  ON up.ProductID = pf.ProductID
LEFT JOIN LIVE.silver_products sp
  ON up.ProductID = sp.ProductID;


