/*
====================================================
Create Views in Gold Layer
====================================================
Purpose:
This script is used to create Views for the Gol layer. Dimension and fact tables for star schema is derived 

Each view is a business object which has combined data from silver layer. The data is cleased, transformed , standardised for Business usage
Usage: Views can be directly queried 
Eg: SELECT * FROM gold.dim_customers 
*/

--------------------Create view for  Dimension Customers--------------

IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS

SELECT
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname firstname,
ci.cst_lastname lastname,
la.cntry country,
ci.cst_marital_status marital_status,
CASE
WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
ELSE COALESCE(ca.gen,'n/a')
END
gender,
ca.bdate birthdate,
ci.cst_create_date created_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid;
GO
--------------------Create view for  Dimension Products--------------

--SELECT * FROM gold.dim_products;
IF OBJECT_ID ('gold.dim_products','V') IS NOT NULL
DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER(ORDER BY i.prd_start_dt,i.product_key) product_key,
i.prd_id product_id,
i.product_key product_number,
i.prd_nm product_name,
i.category_id,
pc.cat category,
pc.subcat subcategory,
pc.maintenance,
i.prd_cost cost,
i.prd_line product_line,
i.prd_start_dt product_start_date
FROM silver.crm_prd_info i
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON i.category_id=pc.id
WHERE i.prd_end_dt is NULL;--Filtering historical data 
GO
--------------------Create view for  Fact Sales--------------

IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

SELECT
s.sls_ord_num order_number,
p.product_key,
c.customer_key,
s.sls_order_dt order_date,
s.sls_ship_dt ship_date,
s.sls_due_dt due_date,
s.sls_sales sales_amount,
s.sls_quantity quantity,
s.sls_price price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products p
ON s.sls_prd_key=p.product_number
LEFT JOIN gold.dim_customers c
ON s.sls_cust_id=c.customer_id

