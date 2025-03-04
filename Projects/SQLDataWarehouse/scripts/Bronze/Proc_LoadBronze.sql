/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================

Purpose:
    This procedure loads raw data into the 'bronze' schema from external CSV files.
    It ensures data consistency by:
    - Truncating existing bronze tables before loading new data.
    - Using `BULK INSERT` to efficiently load data from CSV files.

Parameters:
    None. 
    This procedure does not take input parameters or return values.

===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT '       Initiating Bronze Layer Data Load        ';
        PRINT '================================================';

        -- Loading CRM Tables
        PRINT '------------------------------------------------';
        PRINT '                CRM Data Load                   ';
        PRINT '------------------------------------------------';


		--Customer Information Table
        SET @start_time = GETDATE();
        PRINT 'Processing: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_crm\cust_info.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
        SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';
        


        -- Sales Details Table
        SET @start_time = GETDATE();
        PRINT 'Processing: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.crm_sales_details
		FROM  'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_crm\sales_details.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';


		
        -- Product Information Table
        SET @start_time = GETDATE();
        PRINT 'Processing: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_crm\prd_info.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
	    SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';

		
        -- Loading ERP Tables
        PRINT '------------------------------------------------';
        PRINT '                ERP Data Load                   ';
        PRINT '------------------------------------------------';


		-- Customer Data Table
        SET @start_time = GETDATE();
        PRINT 'Processing: bronze.erp_cust_az12'; 
        TRUNCATE TABLE   bronze.erp_cust_az12;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';


		-- Location Table
        SET @start_time = GETDATE();
        PRINT  'Processing: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_erp\LOC_A101.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';



        -- Product Category Table
        SET @start_time = GETDATE();
        PRINT 'Processing: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '   - Truncated Successfully';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\MaryamHosseinalishir\Desktop\AnalyticsProject\data-engineering-portfolio\Projects\SQLDataWarehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		 FIRSTROW=2,
		 FIELDTERMINATOR=',',
		 TABLOCK
		);
		SET @end_time = GETDATE();
        PRINT '   - Data Inserted Successfully (Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds)';

		-- Completion Summary
        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT '  Bronze Layer Data Load Completed Successfully ';
        PRINT '  Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

	END TRY

	BEGIN CATCH
        PRINT '================================================';
        PRINT '    ERROR OCCURRED DURING BRONZE LAYER LOAD     ';
        PRINT '------------------------------------------------';
		PRINT 'Error Message: ' + COALESCE(ERROR_MESSAGE(), 'Unknown Error');
		PRINT 'Error Number: ' + COALESCE(CAST(ERROR_NUMBER() AS NVARCHAR), ' N/A');
		PRINT 'Error State: ' + COALESCE(CAST(ERROR_STATE() AS NVARCHAR), ' N/A');
        PRINT '================================================';
	END CATCH

END;