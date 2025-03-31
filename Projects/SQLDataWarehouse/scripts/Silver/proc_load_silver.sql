USE DataWarehouse
GO 
INSERT INTO silver.crm_cust_info ( cst_id, cst_key , cst_firstname,cst_lastname ,cst_marital_status, cst_gndr,cst_create_date)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
	CASE WHEN UPPER(TRIM(cst_marital_status))='s'  THEN  'Single'
		 WHEN UPPER(TRIM(cst_marital_status))='m'  THEN  'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr))='f' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gndr))='m' THEN 'Male'
		 ELSE 'n/a'
	END cst_gndr,
    cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;


INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
SELECT 
    prd_id,  -- Selecting the product ID
    
    -- Extract the first 5 characters from `prd_key` and replace '-' with '_', treating it as `cat_id`
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  

    -- Extract everything from the 7th character onward from `prd_key` to redefine it
    SUBSTRING(prd_key, 7 , LEN(prd_key)) AS prd_key,  
    
    prd_nm,  -- Selecting product name
    
    -- If `prd_cost` is NULL, replace it with 0
    ISNULL(prd_cost, 0) AS prd_cost,  
    
    -- Categorizing `prd_line` into descriptive values
    CASE UPPER(TRIM(prd_line))  
        WHEN 'M' THEN 'Mountain'       -- 'M' represents Mountain category
        WHEN 'R' THEN 'Road'           -- 'R' represents Road category
        WHEN 'S' THEN 'other Sales'    -- 'S' represents Other Sales category
        WHEN 'T' THEN 'Touring'        -- 'T' represents Touring category
        ELSE 'n/a'                     -- Default value if no match is found
    END prd_line,
    
    -- Convert `prd_start_dt` to `DATE` type (removing time component)
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    
    -- Determine the end date of each product's active period:
    -- LEAD(prd_start_dt) gets the next row's `prd_start_dt` within the same `prd_key`
    -- Subtracting 1 gives the previous record's `end date`
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_d
FROM bronze.crm_prd_info;


		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)



SELECT DISTINCT  
    sls_ord_num,  
    sls_prd_key,  
    sls_cust_id,  

    -- Convert `sls_order_dt` to a DATE format if it has exactly 8 characters and is not 0
    CASE 
        WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)  
    END AS sls_order_dt,  

    -- Convert `sls_ship_dt` to a DATE format if it has exactly 8 characters and is not 0
    CASE 
        WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)  
    END AS sls_ship_dt,  

    -- Convert `sls_due_dt` to a DATE format if it has exactly 8 characters and is not 0
    CASE 
        WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)  
    END AS sls_due_dt,  

    -- Correct `sls_sales` if it's NULL, non-positive, or doesn't match expected calculation (quantity * price)
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
        THEN sls_quantity * ABS(sls_price)  -- Recalculate as Quantity * Absolute Price
        ELSE sls_sales
    END AS sls_sales,  

    -- Convert `sls_quantity` to NULL if it is 0 (avoid invalid data)
    NULLIF(sls_quantity, 0) AS sls_quantity,  

    -- Correct `sls_price` if it's NULL or non-positive by recalculating from `sls_sales / sls_quantity`
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0  
        THEN sls_sales / sls_quantity  -- Recalculate if necessary
        ELSE sls_price
    END AS sls_price  

FROM bronze.crm_sales_details  

INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)

SELECT 
    -- If `cid` starts with 'NAS', remove the first three characters, else keep it as is
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  
        ELSE cid  
    END AS cid,  -- Renaming `cid` properly after transformation

    -- If `bdate` is in the future (greater than today), set it to NULL, otherwise keep it
    CASE 
        WHEN bdate > GETDATE() THEN NULL  
        ELSE bdate  
    END AS bdate,  -- Renaming `bdate` properly after transformation

    -- Normalize gender values: convert variations of 'Male' and 'Female' to standardized format
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'  
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'  
        ELSE 'n/a'  -- If gender is unknown or not in the expected values, default to 'n/a'
    END AS gen  

FROM bronze.erp_cust_az12;



INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
SELECT 
    -- Remove hyphens ('-') from `cid` to normalize customer IDs
    REPLACE(cid, '-', '') AS cid,  

    -- Standardize country names
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'  -- Convert 'DE' to 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'  -- Normalize US variations
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'  -- Handle missing country values
        ELSE TRIM(cntry)  -- Keep other country values unchanged
    END AS cntry 

FROM bronze.erp_loc_a101;

