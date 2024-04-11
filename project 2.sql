--Ad-hoc tasks
--1. Số lượng đơn hàng và số lượng khách hàng mỗi tháng
SELECT FORMAT_DATE('%Y-%m', created_at) AS month_year,
COUNT(DISTINCT user_id) AS total_users,
COUNT(order_id) AS total_orders
FROM bigquery-public-data.thelook_ecommerce.order_items
WHERE status = 'completed' AND created_at BETWEEN '2019-01-01' AND '2022-04-30'
GROUP BY 1
--2. Giá trị đơn hàng trung bình (AOV) và số lượng khách hàng mỗi tháng
SELECT FORMAT_DATE('%Y-%m', created_at) AS month_year,
COUNT(DISTINCT user_id) AS distinct_users,
SUM(sale_price) / COUNT(DISTINCT order_id) AS average_order_value
FROM 
bigquery-public-data.thelook_ecommerce.order_items
WHERE status = 'completed' 
AND created_at BETWEEN '2019-01-01' AND '2022-04-30'
GROUP BY month_year
--3. Nhóm khách hàng theo độ tuổi
WITH Youngest AS (
SELECT first_name, last_name, gender, age,
'youngest' AS tag,
RANK() OVER(PARTITION BY gender ORDER BY age) AS rn
FROM bigquery-public-data.thelook_ecommerce.users
),
Oldest AS (
SELECT first_name, last_name, gender, age,
'oldest' AS tag,
RANK() OVER(PARTITION BY gender ORDER BY age DESC) AS rn
FROM bigquery-public-data.thelook_ecommerce.users
),
unionn AS(
SELECT first_name, last_name, gender, age, tag
FROM Youngest
WHERE rn = 1
UNION ALL
SELECT first_name, last_name, gender, age, tag
--4. Top 5 sản phẩm mỗi tháng.
WITH ProfitableProducts AS (
SELECT 
DATE_TRUNC(a.created_at, MONTH) AS month_year, a.product_id, b.product_name,
SUM(a.sale_price) AS sales,
SUM(b.cost) AS cost,
SUM(a.sale_price - b.cost) AS profit,
DENSE_RANK() OVER (PARTITION BY DATE_TRUNC(a.created_at, MONTH) ORDER BY SUM(a.sale_price - b.cost) DESC) AS rank_per_month
FROM bigquery-public-data.thelook_ecommerce.order_items a
JOIN bigquery-public-data.thelook_ecommerce.inventory_items b
ON a.product_id = b.product_id
GROUP BY DATE_TRUNC(a.created_at, MONTH) AS month_year, a.product_id, b.product_name
)
SELECT month_year,product_id, product_name, sales, cost, profit, rank_per_month
FROM ProfitableProducts
WHERE rank_per_month <= 5;
FROM Oldest
WHERE rn = 1)
SELECT *, COUNT(*) OVER (PARTITION BY gender, tag) as count
FROM unionn
--5. Doanh thu tính đến thời điểm hiện tại trên mỗi danh mục
SELECT 
DATE_TRUNC(c.created_at, DAY) AS dates,
b.category AS product_categories,
ROUND(SUM(a.sale_price),2) AS revenue
FROM bigquery-public-data.thelook_ecommerce.order_items a
JOIN bigquery-public-data.thelook_ecommerce.orders c
ON a.order_id = c.order_id
JOIN bigquery-public-data.thelook_ecommerce.products b
ON a.product_id = b.id
WHERE c.created_at BETWEEN TIMESTAMP_ADD(TIMESTAMP('2022-04-15'), INTERVAL -90 DAY) AND TIMESTAMP('2022-04-15')
GROUP BY 
DATE_TRUNC(c.created_at, DAY), b.category
ORDER BY dates, product_categories;





-----
WITH TPV AS(
  SELECT 
    EXTRACT(YEAR FROM o.created_at) AS Year,
    EXTRACT(MONTH FROM o.created_at) AS Month,
    ROUND(SUM(oi.sale_price * num_of_item),2) AS tpv
FROM 
    bigquery-public-data.thelook_ecommerce.orders o
JOIN 
    bigquery-public-data.thelook_ecommerce.order_items oi ON o.order_id = oi.order_id
GROUP BY 
    EXTRACT(YEAR FROM o.created_at), EXTRACT(MONTH FROM o.created_at)
ORDER BY 
    Year, Month
),
TPO AS(SELECT 
    EXTRACT(YEAR FROM o.created_at) AS Year,
    EXTRACT(MONTH FROM o.created_at) AS Month,
    COUNT (o.order_id) AS tpo
FROM
    bigquery-public-data.thelook_ecommerce.orders o
JOIN 
    bigquery-public-data.thelook_ecommerce.order_items oi ON o.order_id = oi.order_id
GROUP BY 
    EXTRACT(YEAR FROM o.created_at), EXTRACT(MONTH FROM o.created_at)
ORDER BY 
    Year, Month),
OrderGrowth AS (
    SELECT 
        Year,
        Month,
        (CAST(tpo AS FLOAT64) - CAST(LAG(tpo) OVER (ORDER BY Year, Month) AS FLOAT64)) / CAST(LAG(tpo) OVER (ORDER BY Year, Month) AS FLOAT64) AS Order_growth
    FROM 
        TPO
)
SELECT 
    Year,
    Month,
    COALESCE(Order_growth, 0) AS Order_growth_percentage
FROM 
    OrderGrowth
ORDER BY 
    Year, Month;

SELECT 
  EXTRACT(YEAR FROM o.created_at) AS Year,
  EXTRACT(MONTH FROM o.created_at) AS Month,
  a.category,
  SUM(oi.sale_price * o.num_of_item) AS TPV
FROM 
  bigquery-public-data.thelook_ecommerce.products a
JOIN
  bigquery-public-data.thelook_ecommerce.order_items oi ON oi.product_id = a.id
JOIN
  bigquery-public-data.thelook_ecommerce.orders o ON oi.id = o.order_id
GROUP BY 
  EXTRACT(YEAR FROM o.created_at), EXTRACT(MONTH FROM o.created_at)




