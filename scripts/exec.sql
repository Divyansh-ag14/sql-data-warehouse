/*
===============================================================================
-- Execute Bronze and Silver Procedures
===============================================================================
*/

EXECUTE DataWarehouse.bronze.load_bronze;

-- run the update script to clean data in bronze layer before loading silver layer
-- Note: the update script is idempotent and can be run multiple times without adverse effects
-- scripts/bronze/update_bronze.sql

-- update:  now we have a procedure for it called update_bronze

EXECUTE DataWarehouse.bronze.update_bronze;

EXECUTE DataWarehouse.silver.load_silver;
