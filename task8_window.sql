-- ============================================================
-- TASK 8: Window Functions and Advanced SQL Queries
-- Global Superstore Data Analysis
-- ============================================================

-- ============================================================
-- QUERY 1: Base Aggregation - Total Sales Per Customer
-- Using: GROUP BY
-- ============================================================
SELECT 
    customer_id,
    customer_name,
    SUM(sales) AS total_sales,
    COUNT(*) AS order_count,
    ROUND(AVG(sales), 2) AS avg_order_value
FROM superstore
GROUP BY customer_id, customer_name
ORDER BY total_sales DESC;


-- ============================================================
-- QUERY 2: Rank Customers by Sales Per Region
-- Using: ROW_NUMBER() with PARTITION BY
-- ============================================================
SELECT 
    region,
    customer_id,
    customer_name,
    SUM(sales) AS total_sales,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS sales_rank
FROM superstore
GROUP BY region, customer_id, customer_name
ORDER BY region, sales_rank;


-- ============================================================
-- QUERY 3: Compare RANK() vs DENSE_RANK() - Tie Handling
-- Using: RANK(), DENSE_RANK(), ROW_NUMBER()
-- ============================================================
SELECT 
    region,
    customer_id,
    customer_name,
    SUM(sales) AS total_sales,
    RANK() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS rank_result,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS dense_rank_result,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS row_num_result
FROM superstore
GROUP BY region, customer_id, customer_name
ORDER BY region, total_sales DESC;


-- ============================================================
-- QUERY 4: Running Total Sales
-- Using: SUM() OVER with ORDER BY and ROWS clause
-- ============================================================
SELECT 
    customer_id,
    customer_name,
    order_date,
    sales,
    SUM(sales) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_sales
FROM superstore
ORDER BY customer_id, order_date;


-- ============================================================
-- QUERY 5: Month-over-Month (MoM) Growth Analysis
-- Using: LAG() window function
-- ============================================================
SELECT 
    DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))::DATE AS month,
    SUM(sales) AS monthly_sales,
    LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))) AS previous_month_sales,
    ROUND(
        ((SUM(sales) - LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY')))) 
        / LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))) * 100), 2
    ) AS mom_growth_percent
FROM superstore
GROUP BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))
ORDER BY month;


-- ============================================================
-- QUERY 6: Top 3 Products Per Category
-- Using: DENSE_RANK() with CTE
-- ============================================================
WITH product_rankings AS (
    SELECT 
        category,
        product_name,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(*) AS order_count,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS product_rank
    FROM superstore
    GROUP BY category, product_name
)
SELECT 
    category,
    product_name,
    total_sales,
    total_quantity,
    order_count,
    product_rank
FROM product_rankings
WHERE product_rank <= 3
ORDER BY category, product_rank;


-- ============================================================
-- QUERY 7: Regional Performance Analysis
-- Using: Aggregate functions with window functions
-- ============================================================
SELECT 
    region,
    SUM(sales) AS total_sales,
    SUM(quantity) AS total_units_sold,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(*) AS total_orders,
    ROUND(AVG(sales), 2) AS avg_order_value,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND((SUM(profit) / SUM(sales) * 100), 2) AS profit_margin_percent
FROM superstore
GROUP BY region
ORDER BY total_sales DESC;


-- ============================================================
-- EXPORT QUERIES TO CSV
-- ============================================================

-- Export Query 1: Customer Sales Summary
COPY (
    SELECT 
        customer_id,
        customer_name,
        SUM(sales) AS total_sales,
        COUNT(*) AS order_count,
        ROUND(AVG(sales), 2) AS avg_order_value
    FROM superstore
    GROUP BY customer_id, customer_name
    ORDER BY total_sales DESC
) TO 'C:/ProgramData/01_customer_sales_summary.csv' DELIMITER ',' CSV HEADER;

-- Export Query 2: Top 3 Products By Category
COPY (
    WITH product_rankings AS (
        SELECT 
            category,
            product_name,
            SUM(sales) AS total_sales,
            SUM(quantity) AS total_quantity,
            DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS product_rank
        FROM superstore
        GROUP BY category, product_name
    )
    SELECT 
        category,
        product_name,
        total_sales,
        total_quantity,
        product_rank
    FROM product_rankings
    WHERE product_rank <= 3
    ORDER BY category, product_rank
) TO 'C:/ProgramData/02_top_3_products_by_category.csv' DELIMITER ',' CSV HEADER;

-- Export Query 3: Monthly Sales with MoM Growth
COPY (
    SELECT 
        DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))::DATE AS month,
        SUM(sales) AS monthly_sales,
        LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))) AS previous_month_sales,
        ROUND(
            ((SUM(sales) - LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY')))) 
            / LAG(SUM(sales)) OVER (ORDER BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))) * 100), 2
        ) AS mom_growth_percent
    FROM superstore
    GROUP BY DATE_TRUNC('month', TO_DATE(order_date, 'MM/DD/YYYY'))
    ORDER BY month
) TO 'C:/ProgramData/03_monthly_sales_mom_growth.csv' DELIMITER ',' CSV HEADER;

-- Export Query 4: Regional Performance
COPY (
    SELECT 
        region,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_units_sold,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS total_orders,
        ROUND(AVG(sales), 2) AS avg_order_value,
        ROUND(SUM(profit), 2) AS total_profit,
        ROUND((SUM(profit) / SUM(sales) * 100), 2) AS profit_margin_percent
    FROM superstore
    GROUP BY region
    ORDER BY total_sales DESC
) TO 'C:/ProgramData/04_regional_performance.csv' DELIMITER ',' CSV HEADER;
