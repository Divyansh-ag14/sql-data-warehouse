/*
Check silverlayer to create dimenesion and fact for gold layer
===========================================================================
Quality Checks

===============================================================================
Script Purpose:


*/

SELECT * FROM DataWarehouse.silver.crm_cust_info;

SELECT * FROM DataWarehouse.silver.erp_cust_az12;

SELECT * FROM DataWarehouse.silver.erp_loc_a101;

-- ====================================================================
-- Joining silver.crm_cust_info, silver.erp_cust_az12, silver.erp_loc_a101 to create a dimension table for gold layer
-- ====================================================================


SELECT 
    ci.cst_id, 
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    ci.cst_marital_status,
    ci.cst_gndr,
    ci.dwh_create_date,
    ca.bdate,
    ca.gen,
    la.cntry
FROM DataWarehouse.silver.crm_cust_info ci
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca  
    ON ci.cst_key = ca.cid
LEFT JOIN DataWarehouse.silver.erp_loc_a101 la  
    ON ci.cst_key = la.cid

-- Check for duplicate records based on cst_id after joining the three tables
SELECT cst_id, count(*) as record_count
FROM (
    SELECT 
        ci.cst_id, 
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM DataWarehouse.silver.crm_cust_info ci
    LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca  
        ON ci.cst_key = ca.cid
    LEFT JOIN DataWarehouse.silver.erp_loc_a101 la  
        ON ci.cst_key = la.cid
) AS combined
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for consistency between cst_gndr in crm_cust_info and gen in erp_cust_az12
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen
FROM DataWarehouse.silver.crm_cust_info ci
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca  
    ON ci.cst_key = ca.cid
ORDER BY 1,2;

-- CRM is the master for gender field, so create a new gender field 'new_gen' based on cst_gndr and gen
-- gen can be null because of join, so we will use COALESCE to handle that
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a') -- Use gen from erp_cust_az12 if cst_gndr is 'n/a' and gen is not null
    END AS new_gen
FROM DataWarehouse.silver.crm_cust_info ci
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca  
    ON ci.cst_key = ca.cid
ORDER BY 1,2;


-- Final transformation query to create dimension table for gold layer
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key, -- Surrogate key
    ci.cst_id AS customer_id, 
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a') 
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM DataWarehouse.silver.crm_cust_info ci
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca  
    ON ci.cst_key = ca.cid
LEFT JOIN DataWarehouse.silver.erp_loc_a101 la  
    ON ci.cst_key = la.cid;

-- ====================================================================
-- Joining silver.crm_prd_info and silver.erp_px_cat_g1v2 to create a dimension table for gold layer
-- ====================================================================

SELECT * FROM DataWarehouse.silver.crm_prd_info;

-- Only get current products where prd_end_dt is null
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_start_dt
FROM DataWarehouse.silver.crm_prd_info pn
WHERE pn.prd_end_dt IS NULL;

-- Join with erp_px_cat_g1v2 to get category details
SELECT
    pn.prd_id,
    pn.cat_id,
    pn.prd_key,
    pn.prd_nm,
    pn.prd_cost,
    pn.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM DataWarehouse.silver.crm_prd_info pn
LEFT JOIN DataWarehouse.silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- Check for duplicate records based on prd_key after joining the two tables
SELECT prd_key, COUNT(*) as record_count
FROM (
    SELECT
        pn.prd_id,
        pn.cat_id,
        pn.prd_key,
        pn.prd_nm,
        pn.prd_cost,
        pn.prd_start_dt,
        pc.cat,
        pc.subcat,
        pc.maintenance
    FROM DataWarehouse.silver.crm_prd_info pn
    LEFT JOIN DataWarehouse.silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL
) AS combined
GROUP BY prd_key
HAVING COUNT(*) > 1;

-- Final transformation query to create dimension table for gold layer
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id AS product_id,
    pn.prd_key AS product_key,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM DataWarehouse.silver.crm_prd_info pn
LEFT JOIN DataWarehouse.silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;

-- ====================================================================
-- Creating fact table for gold layer by joining silver.crm_sales_details with the two dimension tables created above
-- ====================================================================

SELECT * FROM DataWarehouse.silver.crm_sales_details;
SELECT * FROM DataWarehouse.gold.dim_products;
SELECT * FROM DataWarehouse.gold.dim_customers;

-- getting keys from dimension tables to create fact table for gold layer using surrogate keys from the dimension tables
SELECT 
    sd.sls_ord_num AS order_number,
    pr.product_key, -- Using product_key from dim_products
    cu.customer_key, -- Using customer_key from dim_customers
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM DataWarehouse.silver.crm_sales_details sd
LEFT JOIN DataWarehouse.gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN DataWarehouse.gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;