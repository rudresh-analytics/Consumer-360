-- ==============================
-- DATABASE
-- ==============================
CREATE DATABASE IF NOT EXISTS Consumer360;
USE Consumer360;

-- ==============================
-- DROP TABLES (for re-run safety)
-- ==============================
DROP VIEW IF EXISTS customer_360_view;
DROP TABLE IF EXISTS returns;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_category;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_customers;

-- ==============================
-- CUSTOMERS
-- ==============================
CREATE TABLE dim_customers (
    customer_id   INT PRIMARY KEY,
    customer_name VARCHAR(100),
    city          VARCHAR(50),
    state         VARCHAR(50),
    signup_date   DATE
);

INSERT INTO dim_customers VALUES
(1,'Amit Shah',    'Ahmedabad','Gujarat',     '2022-01-10'),
(2,'Priya Mehta',  'Mumbai',   'Maharashtra', '2022-03-15'),
(3,'Rahul Verma',  'Delhi',    'Delhi',       '2022-05-20'),
(4,'Sneha Iyer',   'Chennai',  'Tamil Nadu',  '2022-07-18'),
(5,'Karan Patel',  'Surat',    'Gujarat',     '2023-01-05'),
(6,'Neha Singh',   'Lucknow',  'UP',          '2023-02-25'),
(7,'Rohit Sharma', 'Pune',     'Maharashtra', '2023-04-12');

-- ==============================
-- STORE
-- ==============================
CREATE TABLE dim_store (
    store_id   INT PRIMARY KEY,
    store_city VARCHAR(50)
);

INSERT INTO dim_store VALUES
(1,'Ahmedabad'),
(2,'Mumbai'),
(3,'Delhi');

-- ==============================
-- CATEGORY
-- ==============================
CREATE TABLE dim_category (
    category_id   INT PRIMARY KEY,
    category_name VARCHAR(50)
);

INSERT INTO dim_category VALUES
(1,'Electronics'),
(2,'Accessories'),
(3,'Fashion');

-- ==============================
-- PRODUCTS
-- ==============================
CREATE TABLE dim_products (
    product_id   INT PRIMARY KEY,
    product_name VARCHAR(100),
    category_id  INT,
    price        DECIMAL(10,2)
);

INSERT INTO dim_products VALUES
(101,'Laptop',     1, 60000),
(102,'Mobile',     1, 20000),
(103,'Headphones', 2,  2000),
(104,'Shoes',      3,  3000),
(105,'Watch',      3,  5000);

-- ==============================
-- FACT SALES
-- ==============================
CREATE TABLE fact_sales (
    order_id     INT PRIMARY KEY,
    customer_id  INT,
    product_id   INT,
    store_id     INT,
    order_date   DATE,
    quantity     INT,
    total_amount DECIMAL(10,2)
);

INSERT INTO fact_sales VALUES
(1,  1, 101, 1, '2024-01-10', 1, 60000),
(2,  1, 103, 1, '2024-02-15', 2,  4000),
(3,  2, 102, 2, '2024-02-20', 1, 20000),
(4,  3, 104, 3, '2024-03-01', 2,  6000),
(5,  4, 105, 2, '2024-03-10', 1,  5000),
(6,  5, 101, 1, '2024-03-15', 1, 60000),
(7,  6, 103, 3, '2024-03-18', 3,  6000),
(8,  7, 104, 2, '2024-03-20', 1,  3000),
(9,  1, 102, 1, '2024-03-22', 1, 20000),
(10, 2, 105, 2, '2024-03-25', 2, 10000);

-- ==============================
-- RETURNS
-- ==============================
CREATE TABLE returns (
    return_id   INT PRIMARY KEY,
    order_id    INT,
    return_date DATE,
    reason      VARCHAR(100)
);

-- ============================================================
-- SECTION 1: BASIC TEST QUERIES (your original work - correct)
-- ============================================================

-- View all tables
SELECT * FROM dim_customers;
SELECT * FROM dim_store;
SELECT * FROM dim_products;
SELECT * FROM fact_sales;

-- Full JOIN across all 4 tables
SELECT
    fs.order_id,
    dc.customer_name,
    dp.product_name,
    ds.store_city,
    fs.total_amount
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
JOIN dim_products  dp ON fs.product_id  = dp.product_id
JOIN dim_store     ds ON fs.store_id    = ds.store_id;

-- Basic RFM query (your original - correct)
SELECT
    customer_id,
    MAX(order_date)                          AS last_purchase,
    DATEDIFF(CURDATE(), MAX(order_date))     AS recency,
    COUNT(order_id)                          AS frequency,
    SUM(total_amount)                        AS monetary
FROM fact_sales
GROUP BY customer_id;

-- ============================================================
-- SECTION 2: NEW - SQL VIEW (Single Customer View)
-- Required by Week 1 deliverable
-- A VIEW is a saved query. Instead of writing this big JOIN
-- every time, you just write: SELECT * FROM customer_360_view
-- ============================================================

CREATE OR REPLACE VIEW customer_360_view AS
SELECT
    dc.customer_id,
    dc.customer_name,
    dc.city,
    dc.state,
    dc.signup_date,
    COUNT(fs.order_id)                           AS total_orders,
    SUM(fs.total_amount)                         AS total_spent,
    MAX(fs.order_date)                           AS last_purchase_date,
    DATEDIFF(CURDATE(), MAX(fs.order_date))      AS days_since_purchase,
    ROUND(SUM(fs.total_amount) / COUNT(fs.order_id), 2) AS avg_order_value
FROM dim_customers dc
LEFT JOIN fact_sales fs ON dc.customer_id = fs.customer_id
GROUP BY
    dc.customer_id,
    dc.customer_name,
    dc.city,
    dc.state,
    dc.signup_date;

-- Test the view
SELECT * FROM customer_360_view;

-- ============================================================
-- SECTION 3: NEW - WINDOW FUNCTIONS
-- Required: RANK, DENSE_RANK, LAG, LEAD
-- Window functions rank/compare rows WITHOUT removing them
-- from the result — that is what makes them special
-- ============================================================

-- RANK & DENSE_RANK: rank customers by spend and frequency
SELECT
    dc.customer_name,
    SUM(fs.total_amount)                                        AS total_spent,
    COUNT(fs.order_id)                                         AS total_orders,

    -- RANK: if two customers tie, next rank is skipped (1,1,3)
    RANK()       OVER (ORDER BY SUM(fs.total_amount) DESC)     AS spend_rank,

    -- DENSE_RANK: if two customers tie, next rank is NOT skipped (1,1,2)
    DENSE_RANK() OVER (ORDER BY SUM(fs.total_amount) DESC)     AS spend_dense_rank,

    -- Rank by frequency separately
    RANK()       OVER (ORDER BY COUNT(fs.order_id) DESC)       AS frequency_rank
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
GROUP BY dc.customer_name;


-- LAG & LEAD: compare each order to the previous/next order
-- LAG looks BACKWARDS (previous row), LEAD looks FORWARDS (next row)
SELECT
    dc.customer_name,
    fs.order_date,
    fs.total_amount,

    -- What did this customer spend on their PREVIOUS order?
    LAG(fs.total_amount)  OVER (
        PARTITION BY fs.customer_id
        ORDER BY fs.order_date
    ) AS previous_order_amount,

    -- What did this customer spend on their NEXT order?
    LEAD(fs.total_amount) OVER (
        PARTITION BY fs.customer_id
        ORDER BY fs.order_date
    ) AS next_order_amount,

    -- How many days gap between this order and the previous one?
    DATEDIFF(
        fs.order_date,
        LAG(fs.order_date) OVER (
            PARTITION BY fs.customer_id
            ORDER BY fs.order_date
        )
    ) AS days_since_last_order
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
ORDER BY fs.customer_id, fs.order_date;

-- ============================================================
-- SECTION 4: NEW - CTEs (Common Table Expressions)
-- A CTE uses the WITH keyword. It is like a temporary table
-- that only exists for that one query. It makes complex
-- queries much easier to read and understand.
-- ============================================================

-- CTE Example 1: Clean RFM calculation using WITH
WITH rfm_base AS (
    -- Step 1: calculate raw R, F, M values
    SELECT
        customer_id,
        DATEDIFF(CURDATE(), MAX(order_date)) AS recency,
        COUNT(order_id)                       AS frequency,
        SUM(total_amount)                     AS monetary
    FROM fact_sales
    GROUP BY customer_id
),
rfm_scored AS (
    -- Step 2: add a label based on recency (uses rfm_base above)
    SELECT
        rfm_base.*,
        CASE
            WHEN recency <= 30  THEN 'Recent buyer'
            WHEN recency <= 90  THEN 'Moderate'
            ELSE                     'Lapsed'
        END AS recency_label,
        CASE
            WHEN monetary >= 60000 THEN 'High value'
            WHEN monetary >= 10000 THEN 'Mid value'
            ELSE                        'Low value'
        END AS monetary_label
    FROM rfm_base
)
-- Step 3: join with customer names for the final output
SELECT
    rfm_scored.*,
    dc.customer_name,
    dc.city,
    dc.state
FROM rfm_scored
JOIN dim_customers dc ON rfm_scored.customer_id = dc.customer_id
ORDER BY monetary DESC;


-- CTE Example 2: Top products per category using WITH + RANK
WITH product_sales AS (
    SELECT
        dp.category_id,
        dc2.category_name,
        dp.product_name,
        SUM(fs.total_amount)  AS total_revenue,
        COUNT(fs.order_id)    AS times_sold,
        RANK() OVER (
            PARTITION BY dp.category_id
            ORDER BY SUM(fs.total_amount) DESC
        ) AS rank_in_category
    FROM fact_sales fs
    JOIN dim_products  dp  ON fs.product_id  = dp.product_id
    JOIN dim_category  dc2 ON dp.category_id = dc2.category_id
    GROUP BY dp.category_id, dc2.category_name, dp.product_name
)
SELECT *
FROM product_sales
WHERE rank_in_category = 1;   -- only show the top product per category

-- ============================================================
-- SECTION 5: NEW - STORED PROCEDURE
-- A stored procedure is saved SQL code you can run anytime
-- by just calling its name. Used for automated reporting.
-- ============================================================

DROP PROCEDURE IF EXISTS get_rfm_report;

DELIMITER $$

CREATE PROCEDURE get_rfm_report()
BEGIN
    -- This procedure gives a full RFM report with segment labels
    -- Call it anytime with: CALL get_rfm_report();
    WITH rfm AS (
        SELECT
            customer_id,
            DATEDIFF(CURDATE(), MAX(order_date)) AS recency,
            COUNT(order_id)                       AS frequency,
            SUM(total_amount)                     AS monetary
        FROM fact_sales
        GROUP BY customer_id
    )
    SELECT
        dc.customer_name,
        dc.city,
        rfm.recency,
        rfm.frequency,
        rfm.monetary,
        CASE
            WHEN rfm.recency <= 30 AND rfm.frequency >= 2 THEN 'Champion'
            WHEN rfm.frequency >= 2                        THEN 'Loyal Customer'
            WHEN rfm.recency <= 30                         THEN 'Recent Customer'
            WHEN rfm.recency > 90                          THEN 'Churn Risk'
            ELSE                                                'Average'
        END AS segment
    FROM rfm
    JOIN dim_customers dc ON rfm.customer_id = dc.customer_id
    ORDER BY rfm.monetary DESC;
END$$

DELIMITER ;

-- Run the stored procedure
CALL get_rfm_report();