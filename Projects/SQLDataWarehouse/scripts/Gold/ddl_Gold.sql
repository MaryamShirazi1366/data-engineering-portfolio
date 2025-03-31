/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema).

    Each view transforms and integrates data from the Silver layer to produce 
    business-ready datasets optimized for analytics and reporting.

Usage:
    - These views can be queried directly for dashboards and data analysis.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Generates a unique surrogate key
    ci.cst_id                              AS customer_id,   -- Business key for customers
    ci.cst_key                             AS customer_number,
    ci.cst_firstname                       AS first_name,
    ci.cst_lastname                        AS last_name,
    la.cntry                               AS country, -- Country information from ERP data
    ci.cst_marital_status                  AS marital_status,
    
    -- Gender prioritization:
    -- 1. If gender is available in CRM, use it.
    -- 2. Otherwise, fallback to the ERP source.
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE COALESCE(ca.gen, 'n/a')  
    END                                  AS gender,
    
    ca.bdate                             AS birthdate, -- Birthdate from ERP system
    ci.cst_create_date                   AS create_date -- Customer registration date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid -- Joining with ERP customer data for additional attributes
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid; -- Joining location details from ERP
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,      -- Business identifier for the product
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,        -- Product category (from ERP data)
    pc.subcat       AS subcategory,     -- Product subcategory
    pc.maintenance  AS maintenance,     -- Maintenance classification
    pn.prd_cost     AS cost,            -- Product cost value
    pn.prd_line     AS product_line,    -- Type of product (e.g., Road, Mountain, etc.)
    pn.prd_start_dt AS start_date       -- Start date of product availability
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id -- Enriching product data with category & subcategory
WHERE pn.prd_end_dt IS NULL; -- Excludes discontinued products from the dataset
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number, -- Sales order identifier
    pr.product_key  AS product_key,  -- Foreign key to dim_products
    cu.customer_key AS customer_key, -- Foreign key to dim_customers
    sd.sls_order_dt AS order_date,   -- Date when order was placed
    sd.sls_ship_dt  AS shipping_date, -- Expected shipping date
    sd.sls_due_dt   AS due_date,     -- Payment due date
    sd.sls_sales    AS sales_amount, -- Total sales amount
    sd.sls_quantity AS quantity,     -- Number of products sold
    sd.sls_price    AS price         -- Unit price of the product
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number -- Linking product details from dimension table
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id; -- Linking customer details from dimension table
GO
