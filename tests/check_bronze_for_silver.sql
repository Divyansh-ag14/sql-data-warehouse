/*
Checking and transforming bronze layer tables for silver layer
===============================================================================
Script Purpose:
    This script performs various quality checks and transformations on the
    'bronze' layer tables to prepare them for loading into the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
    
Usage Notes:
    - Run these checks and transformations before loading data into the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
*/


-- ====================================================================
-- Checking 'bronze.crm_cust_info'
-- ====================================================================

SELECT * FROM DataWarehouse.bronze.crm_cust_info;

-- To check for duplicates or nulls in primary key
SELECT 
    cst_id,
    COUNT(*) 
FROM DataWarehouse.bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- To check for unwanted spaces
SELECT 
    cst_key 
FROM DataWarehouse.bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- To check for data standardization & consistency
SELECT DISTINCT 
    cst_marital_status 
FROM DataWarehouse.bronze.crm_cust_info;

-- Final transformation query for silver layer
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname, -- Remove leading/trailing spaces from first name
    TRIM(cst_lastname) AS cst_lastname, -- Remove leading/trailing spaces from last name
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
        END AS cst_marital_status, -- Normalize marital status values to readable format
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
        END AS cst_gndr, -- Normalize gender values to readable format
        cst_create_date
        FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                    FROM bronze.crm_cust_info
                    WHERE cst_id IS NOT NULL
            ) t
            WHERE flag_last = 1

-- ====================================================================
-- Checking 'bronze.crm_prd_info'
-- ====================================================================

SELECT * FROM DataWarehouse.bronze.crm_prd_info;

-- To check for duplicates or nulls in primary key
SELECT 
    prd_id,
    COUNT(*) 
FROM DataWarehouse.bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- To check for unwanted spaces
SELECT 
    prd_nm 
FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- To check for nulls or negative values in cost
SELECT 
    prd_cost 
FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- To check for data standardization & consistency
SELECT DISTINCT 
    prd_line 
FROM DataWarehouse.bronze.crm_prd_info;

-- To check for invalid date ranges
SELECT 
    * 
FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- Final transformation query for silver layer
SELECT 
	prd_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- Remove category ID from prd_key
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID from prd_key
	prd_nm,
	ISNULL(prd_cost, 0) as prd_cost,
	CASE 
	    WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	    WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
    WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
    WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
    ELSE 'n/a'
END as prd_line,
CAST(prd_start_dt AS DATE) as prd_start_dt, -- Convert datetime to DATE format
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt -- Set end date as day before next start date
from DataWarehouse.bronze.crm_prd_info

-- ====================================================================
-- Checking 'bronze.crm_sales_details'
-- ====================================================================

SELECT * FROM DataWarehouse.bronze.crm_sales_details;

-- To check for referential integrity issues
SELECT * FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM DataWarehouse.silver.crm_cust_info)
   OR sls_prd_key NOT IN (SELECT prd_key FROM DataWarehouse.silver.crm_prd_info); 

-- To identify invalid order date formats
SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_order_dt<=0 OR LEN(sls_order_dt) != 8 

-- Check if order date is higher than ship date or due date
SELECT * FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check if sales = quantity * price
SELECT sls_sales, sls_quantity, sls_price
 FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL 
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0;

-- Final transformation query for silver layer
SELECT 
    sls_ord_num
    sls_prd_key,
    sls_cust_id,

    CASE WHEN sls_order_dt<=0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
    END AS sls_order_date, -- Convert integer date to DATE format, set invalid dates to NULL

    CASE WHEN sls_ship_dt<=0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
    END AS sls_ship_dt, -- Convert integer date to DATE format, set invalid dates to NULL
    
    CASE WHEN sls_due_dt<=0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
    END AS sls_due_dt, -- Convert integer date to DATE format, set invalid dates to NULL
    
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    
    sls_quantity,
    
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM DataWarehouse.bronze.crm_sales_details;

-- ====================================================================
-- Checking 'bronze.erp_cust_az12'
-- ====================================================================     

SELECT * FROM DataWarehouse.bronze.erp_cust_az12;

-- To check for nulls or invalid customer IDs
SELECT
    cid
FROM DataWarehouse.bronze.erp_cust_az12
WHERE cid IS NULL OR cid LIKE '%NAS%'; -- we have to remove 'NAS' prefix in final transformation

-- To check for referential integrity issues
SELECT
    CASE 
        WHEN cid Like '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present to join with crm.cust_info
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM DataWarehouse.bronze.erp_cust_az12
WHERE CASE 
        WHEN cid Like '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present to join with crm.cust_info
        ELSE cid
        END 
    NOT IN (SELECT cst_key FROM DataWarehouse.silver.crm_cust_info);

-- To identify out-of-range birthdates
SELECT DISTINCT 
    bdate
FROM DataWarehouse.bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE();

-- To check data standardization & consistency
-- Note: run the update script in 'scripts/bronze/update_bronze.sql' before checking this
SELECT DISTINCT 
    gen
FROM DataWarehouse.bronze.erp_cust_az12;

-- Final transformation query for silver layer
SELECT
    CASE 
        WHEN cid Like '%NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present to join with crm.cust_info
        ELSE cid
    END AS cid,
    
    CASE 
        WHEN bdate > GETDATE() THEN NULL
        ELSE bdate
    END AS bdate, -- Set future birthdates to NULL
    
    CASE 
        WHEN UPPER(TRIM(gen)) = 'F' OR UPPER(TRIM(gen)) = 'FEMALE' THEN 'Female'
        WHEN UPPER(TRIM(gen)) = 'M' OR UPPER(TRIM(gen)) = 'MALE' THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM DataWarehouse.bronze.erp_cust_az12;

-- ====================================================================
-- Checking 'bronze.erp_loc_a101'
-- ====================================================================

SELECT * FROM DataWarehouse.bronze.erp_loc_a101;

-- To check for nulls in primary key
SELECT * From DataWarehouse.bronze.erp_loc_a101
WHERE cid is NULL;

-- Remove hyphens from 'cid' to match with 'crm_cust_info' table 
SELECT
REPLACE(cid, '-', '') AS cid, -- join with crm_cust_info on cst_key
cntry 
FROM DataWarehouse.bronze.erp_loc_a101;

-- To check for referential integrity issues
SELECT
REPLACE(cid, '-', '') AS cid, -- join with crm_cust_info on cst_key
cntry 
FROM DataWarehouse.bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM DataWarehouse.silver.crm_cust_info);

-- To check data standardization & consistency
-- Note: run the update script in 'scripts/bronze/update_bronze.sql' before checking this or run the update_bronze procedure in exec.sql
SELECT DISTINCT 
    cntry 
FROM DataWarehouse.bronze.erp_loc_a101;

SELECT
CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN UPPER(TRIM(cntry)) IN('US', 'USA', 'UNITED STATES') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
    END AS cntry
FROM DataWarehouse.bronze.erp_loc_a101;

-- Final transformation query for silver layer
SELECT
    REPLACE(cid, '-', '') AS cid, -- join with crm_cust_info on cst_key
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN('US', 'USA', 'UNITED STATES') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM DataWarehouse.bronze.erp_loc_a101;

-- ====================================================================
-- Checking 'bronze.erp_px_cat_g1v2'
-- ====================================================================

SELECT * FROM DataWarehouse.bronze.erp_px_cat_g1v2;

-- To check for nulls in primary key
SELECT * from DataWarehouse.bronze.erp_px_cat_g1v2
WHERE id is NULL; -- this id will bee joined with cat_id in silver.crm_prd_info

-- To check for unwanted spaces
SELECT * FROM DataWarehouse.bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

SELECT 
DISTINCT cat 
from DataWarehouse.bronze.erp_px_cat_g1v2;

SELECT 
DISTINCt subcat
FROM DataWarehouse.bronze.erp_px_cat_g1v2;

-- Note: run the update script in 'scripts/bronze/update_bronze.sql' before checking this or run the update_bronze procedure in exec.sql
SELECT 
DISTINCT maintenance
FROM DataWarehouse.bronze.erp_px_cat_g1v2;