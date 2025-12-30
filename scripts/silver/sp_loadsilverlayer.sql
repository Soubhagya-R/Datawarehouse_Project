/*
==========================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver layer)
==========================================================
Stored Procedure to load the Silver layer
Purpose:
 We have cleansed,transformed and standardized the data from Bronze layer and loaded them to Silver layer
 
Actions Performed:
			-Truncate the silver tables
			-Inser data to silver schema tables from cleansed data of bronze tables

Parameter:
NONE

USAGE:
EXEC silver.load_silver;

*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_date DATETIME, @end_date DATETIME, @batch_start DATETIME, @batch_end DATETIME
	BEGIN TRY
			
		SET @batch_start=GETDATE();
		PRINT'======================================================';
		PRINT'LOADING SILVER LAYER';
		PRINT'======================================================';
		PRINT'>>Loading CRM Tables';
		SET @start_date=GETDATE();
		PRINT'Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT'Inserting Into Table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date
				)

				SELECT 
					cst_id,
					cst_key,
					TRIM(cst_firstname) cst_firstname,
					TRIM(cst_lastname) cst_lastname,
					CASE UPPER(TRIM(cst_marital_status))
						WHEN 'M' THEN 'Married'
						WHEN 'S' THEN 'Single'
						ELSE 'n/a'
					END cst_marital_status,--Normalize the values to readable format
					CASE UPPER(TRIM(cst_gndr))
						WHEN 'M' THEN 'Male'
						WHEN 'F' THEN 'Female'
						ELSE 'n/a'
					END cst_gndr,--Normalize the values to readable format
					cst_create_date
				FROM
				(
				SELECT *, 
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) AS flag_latest
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL)t --selecting the recent record for every customer
			WHERE t.flag_latest=1;
		SET @end_date= GETDATE();

		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';
		-----------------------------------------------------------------------------

		SET @start_date=GETDATE();

		PRINT'Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT'Inserting Into Table: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
		prd_id,
		category_id,
		product_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') category_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) product_key,
		prd_nm,
		ISNULL(prd_cost,0) prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line,--Data Normalization
		prd_start_dt,
		DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION bY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt--Data Enrichment
		FROM bronze.crm_prd_info;
		SET @end_date=GETDATE();
		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';

		----------------------------------------------------------------------------------------------------

		SET @start_date=GETDATE();
		PRINT'Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT'Inserting Into Table: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
		(sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN len(sls_order_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
		END sls_order_dt,
		CASE WHEN len(sls_ship_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
		END sls_ship_dt,
		CASE WHEN len(sls_due_dt)!=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
		END sls_due_dt,
		CASE WHEN (sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=ABS(sls_quantity)*ABS(sls_price))
			THEN ABS(sls_quantity)*ABS(sls_price)
			ELSE sls_sales
		END sls_sales,--Recalculate Sales when its value is Invalid
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
			THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
		END sls_price--Recalculate from Sales and quantity when value is incorrect
		FROM bronze.crm_sales_details;

		SET @end_date=GETDATE();
		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';

		----------------------------------------------------------------------------------
		PRINT'>>Loading ERP Tables';
		SET @start_date=GETDATE();

		PRINT'Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT'Inserting Into Table: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)

		SELECT 
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END cid,--Remove NAS from cid if present
		CASE
			WHEN bdate>GETDATE() THEN NULL
			ELSE bdate
		END bdate,--Set Invalid future birthdates to NULL
		CASE 
			WHEN UPPER(gen)='M' THEN 'Male'
			WHEN UPPER(gen)='F' THEN 'Female'
			WHEN NULLIF(TRIM(gen),'') IS NULL THEN 'n/a'
			ELSE gen
		END gen--Normalised the values and handled NULLs
		FROM bronze.erp_cust_az12;

		SET @end_date=GETDATE();
		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';

		-----------------------------------------------------------------------------------
		SET @start_date=GETDATE();
		PRINT'Truncating Table: silver.erp_loc_a101';

		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT'Inserting Into Table: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid,
		cntry)

		SELECT 
		REPLACE(cid,'-','') cid,--Data Standardization
		CASE 
			WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
			WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
			Else cntry
		END cntry--Data Normalization
		FROM bronze.erp_loc_a101;

		SET @end_date=GETDATE();
		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';


		----------------------------------------------------------------------------------------

		SET @start_date=GETDATE();
		PRINT'Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT'Inserting Into Table: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance)

		SELECT 
		*
		FROM bronze.erp_px_cat_g1v2 ;

		SET @end_date=GETDATE();
		PRINT'Time taken for Updating the table: '+CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR)+' seconds';
		PRINT'--------------------------------------------------------------------------';
		PRINT'********** SILVER LAYER LOAD COMPLETED SUCCESSFULLY**********';
		SET @batch_end=GETDATE();

		PRINT'Total Batch Processing time: '+CAST(DATEDIFF(second,@batch_start,@batch_end) AS VARCHAR)+' seconds';

	END TRY

	BEGIN CATCH
		PRINT'###############################################';
		PRINT'ERROR ENCOUNTERED IN SILVER LAYER LOADING';
		PRINT'-----------------------------------------------';
		PRINT'Error Message: '+ERROR_MESSAGE();
		PRINT'Error Number: '+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error State: '+CAST(ERROR_STATE() AS NVARCHAR);

		PRINT'###############################################';
	END CATCH

END
