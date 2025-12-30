/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze layer)
=============================================================
Pupose:
This stored procedure loads the data from external source CSV files into Bronze layer tables in bronze schema using Bulk load.
Actions Performed:
			-Truncate the tables
			-'BULK INSERT' data from CSV files to tables in bronze schema

Parameter:
NONE

USAGE:
EXEC bronze.load_bronze;

*/

CREATE OR ALTER   PROCEDURE [bronze].[load_bronze] AS
BEGIN
DECLARE @starttime DATETIME, @endtime DATETIME,@bronzeloadstart DATETIME, @bronzeloadend DATETIME;
	BEGIN TRY
		SET @bronzeloadstart=GETDATE();
		PRINT'==============================================================';
		PRINT'DATA LOADING IN BRONZE LAYER';
		PRINT'==============================================================';

		PRINT'***********************';
		PRINT'Loading CRM Tables';
		PRINT'***********************';

		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.crm_cust_info table ';

		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>>Inserting bronze.crm_cust_info table ';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.crm_cust_info table';
		SET @endtime=GETDATE();
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'
		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.crm_prd_info table';

		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT'>>Inserting bronze.crm_prd_info table ';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.crm_prd_info table';
		SET @endtime=GETDATE();
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'

		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.crm_sales_details table';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT'>>Inserting bronze.crm_sales_details table ';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.crm_sales_details table';
		SET @endtime=GETDATE();
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'

		PRINT'***********************';
		PRINT'Loading ERP Tables';
		PRINT'***********************';

		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.erp_cust_az12 table';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT'>>Inserting bronze.erp_cust_az12 table ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.erp_cust_az12 table';
		SET @endtime=GETDATE();
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'

		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.erp_loc_a101 table';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>>Inserting bronze.erp_loc_a101 table ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.erp_loc_a101 table';
		SET @endtime=GETDATE()
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'

		SET @starttime=GETDATE();
		PRINT'>>Truncating bronze.erp_px_cat_g1v2 table';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>>Inserting bronze.erp_px_cat_g1v2 table ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\aswin\OneDrive\Documents\Data Engineering course by Baara\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		PRINT'>>Completed for bronze.erp_px_cat_g1v2';
		SET @endtime=GETDATE();
		PRINT'>>Time taken: '+CAST(DATEDIFF(second,@starttime,@endtime) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------'

		PRINT'******BRONZE LAYER LOAD COMPLETED SUCCESSFULLY******';
		SET @bronzeloadend=GETDATE()
		PRINT'Total time taken for loading Bronze Layer: '+CAST(DATEDIFF(second,@bronzeloadstart,@bronzeloadend) AS NVARCHAR)+ ' seconds'
	END TRY

	BEGIN CATCH
	
	PRINT'###############################################';
	PRINT'ERROR ENCOUNTERED IN BRONZE LAYER LOADING';
	PRINT'-----------------------------------------------';
	PRINT'Error Message: '+ERROR_MESSAGE();
	PRINT'Error Number: '+CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error State: '+CAST(ERROR_STATE() AS NVARCHAR);

	PRINT'###############################################';
	
	END CATCH
END
