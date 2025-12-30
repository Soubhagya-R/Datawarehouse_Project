SELECT * FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key=p.product_key
LEFT JOIN  gold.dim_customers c
ON s.customer_key = c.customer_key
WHERE s.product_key IS NULL
OR s.customer_key IS NULL
