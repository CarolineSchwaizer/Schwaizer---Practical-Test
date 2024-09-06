-- Find the top 5 most popular genres by total sales. 
-- popular_genres.csv
SELECT
	g.name
	,SUM(i.total) AS total_sales
FROM invoice i
JOIN invoice_line il
	ON i.invoice_id = il.invoice_id
JOIN track t
	ON il.track_id = t.track_id
JOIN genre g
	ON t.genre_id = g.genre_id
GROUP BY g.name
ORDER BY total_sales DESC
LIMIT 5;

-- Calculate the average invoice total by country. 
-- average_invoice_total_by_country.csv
SELECT
	billing_country
	,CAST(AVG(total) AS DECIMAL(10,2)) AS average_invoice
FROM invoice
GROUP BY billing_country;

-- Identify the top 3 most valued customers based on the total sum of invoices. 
-- most_valued_customers.csv
SELECT
	c.customer_id
	,c.first_name
	,c.last_name
	,CAST(SUM(i.total) AS DECIMAL(26,2)) AS total_amount
FROM customer c
JOIN invoice i
	ON c.customer_id = i.customer_id
GROUP BY 
	c.customer_id
	,c.first_name
	,c.last_name
ORDER BY total_amount DESC
LIMIT 3;

-- Generate a report listing all employees who have sold over a specified amount. 
PREPARE report (INTEGER) AS
SELECT
	e.employee_id
	,e.first_name
	,e.last_name
	,CAST(SUM(i.total) AS DECIMAL(26,2)) AS total_amount
FROM employee e
JOIN customer c
	ON e.employee_id = c.support_rep_id
JOIN invoice i
	ON c.customer_id = i.customer_id
GROUP BY 
	e.employee_id
	,e.first_name
	,e.last_name
HAVING SUM(i.total) >= $1;

-- employees_over_100.csv
EXECUTE report (100);

-- employees_over_500.csv
EXECUTE report (500);

-- employees_over_800.csv
EXECUTE report (800);