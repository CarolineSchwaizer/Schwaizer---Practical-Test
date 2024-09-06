WITH purchase_value AS(
	SELECT 
		customerid
		,quantity * unitprice AS purchase_value
	FROM retail.online_retail
	WHERE customerid <> '0'
    AND invoiceno NOT LIKE 'C%'
	)
SELECT
	customerid
	,CAST(SUM(purchase_value) AS DECIMAL(26,2)) AS total_purchase_amount
FROM purchase_value
GROUP BY customerid
ORDER BY total_purchase_amount DESC
LIMIT 10;

SELECT
	stockcode
	,COUNT(invoiceno) AS total_orders
FROM retail.online_retail
WHERE invoiceno NOT LIKE 'C%'
GROUP BY stockcode
ORDER BY total_orders DESC
LIMIT 100;

WITH purchase_value AS(
	SELECT 
		invoicedate
		,quantity * unitprice AS purchase_value
	FROM retail.online_retail
    WHERE invoiceno NOT LIKE 'C%'
	) 
SELECT
	DATE_PART('year', invoicedate) AS Year
	,DATE_PART('month', invoicedate) AS Month
	,CAST(SUM(purchase_value) AS DECIMAL(26,2)) AS Monthly_Revenue
FROM purchase_value
GROUP BY DATE_PART('year', invoicedate), DATE_PART('month', invoicedate);
