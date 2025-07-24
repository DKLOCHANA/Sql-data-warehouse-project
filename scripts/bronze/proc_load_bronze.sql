/*
load data to bronze layer
uses bulk insert to insert data from csv
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE 
        @total_start_time DATETIME, 
        @total_end_time DATETIME,
        @start_time DATETIME, 
        @end_time DATETIME;

    BEGIN TRY
        SET @total_start_time = GETDATE();
        PRINT('========================================');
        PRINT('LOADING BRONZE LAYER...');
        PRINT('========================================');

        -- CRM TABLES -------------------------
        PRINT('========================================');
        PRINT('LOADING CRM TABLES...');
        PRINT('========================================');

        -- bronze.crm_cust_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_cust_info LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- bronze.crm_prd_info
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_prd_info LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- bronze.crm_sales_details
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.crm_sales_details LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- ERP TABLES -------------------------
        PRINT('========================================');
        PRINT('LOADING ERP TABLES...');
        PRINT('========================================');

        -- bronze.erp_cust_az12
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_cust_az12 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- bronze.erp_loc_a101
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_loc_a101 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\LOCHANA\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR =',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> bronze.erp_px_cat_g1v2 LOAD DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) +' SECONDS..'

        -- TOTAL TIME
        SET @total_end_time = GETDATE();
        PRINT('========================================');
        PRINT '>> TOTAL BRONZE LOAD DURATION: ' + CAST(DATEDIFF(second, @total_start_time, @total_end_time) as NVARCHAR) +' SECONDS..'
        PRINT('========================================');

    END TRY
    BEGIN CATCH
        PRINT('========================================');
        PRINT ('ERROR OCCURRED WHILE LOADING BRONZE LAYER..');
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE: ' + CAST (ERROR_STATE() AS NVARCHAR);
        PRINT('========================================');
    END CATCH
END
