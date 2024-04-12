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






--Tạo metric trước khi dựng dashboard
--TPV
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
--TPO
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
--Total_cost
Total_cost AS (SELECT 
    EXTRACT(YEAR FROM o.created_at) AS Year,
    EXTRACT(MONTH FROM o.created_at) AS Month,
    ROUND(SUM(a.cost),2) AS Total_cost
FROM 
  bigquery-public-data.thelook_ecommerce.products a
JOIN
  bigquery-public-data.thelook_ecommerce.order_items oi ON oi.product_id = a.id
JOIN
  bigquery-public-data.thelook_ecommerce.orders o ON oi.id = o.order_id
GROUP BY 
    EXTRACT(YEAR FROM o.created_at), EXTRACT(MONTH FROM o.created_at)
ORDER BY 
    Year, Month),
-- Tính tổng lợi nhuận mỗi tháng
TotalProfit AS (
    SELECT 
        a.Year,
        a.Month,
        ROUND((b.tpv - a.Total_cost),2) AS Total_profit
    FROM 
        Total_cost a
    JOIN 
        TPV b ON a.Year = b.Year AND a.Month = b.Month
),
--Profit_to_cost_ratio
Profit_to_cost_ratio AS(SELECT 
    a.Year,
    a.Month,
    CASE 
        WHEN b.Total_cost = 0 THEN 0
        ELSE ROUND((a.Total_profit / b.Total_cost),2)
    END AS Profit_to_costratio
FROM 
    TotalProfit a
JOIN 
    Total_cost b ON a.Year = b.Year AND a.Month = b.Month
ORDER BY 
    Year, Month)
--main
SELECT 
    MR.Year AS Year,
    MR.Month AS Month,
    MR.tpv AS TPV,
    MO.tpo AS TPO,
    CASE 
        WHEN LAG(MR.tpv) OVER (ORDER BY MR.Year, MR.Month) IS NULL THEN 0 || '%'
        ELSE ROUND((MR.tpv - LAG(MR.tpv) OVER (ORDER BY MR.Year, MR.Month)) / LAG(MR.tpv) OVER (ORDER BY MR.Year, MR.Month),2) || '%'
    END AS Revenue_growth,
    CASE 
        WHEN LAG(MO.tpo) OVER (ORDER BY MO.Year, MO.Month) IS NULL THEN 0 || '%'
        ELSE ROUND((MO.tpo - LAG(MO.tpo) OVER (ORDER BY MO.Year, MO.Month)) / LAG(MO.tpo) OVER (ORDER BY MO.Year, MO.Month),2) || '%'
    END AS Order_growth,
    TC.Total_cost AS Total_cost,
    TP.Total_profit AS Total_profit,
    CASE 
        WHEN TC.Total_cost = 0 THEN 0
        ELSE ROUND((TP.Total_profit / TC.Total_cost),2)
    END AS Profit_to_cost_ratio
FROM 
    TPV MR
JOIN 
    TPO MO ON MR.Year = MO.Year AND MR.Month = MO.Month
JOIN 
    Total_cost TC ON MR.Year = TC.Year AND MR.Month = TC.Month
JOIN 
    TotalProfit TP ON MR.Year = TP.Year AND MR.Month = TP.Month 
ORDER BY 
    MR.Year, MR.Month;



--Cohort
WITH retail AS(
    SELECT a.*, b.*, c.*, d.*
    FROM bigquery-public-data.thelook_ecommerce.orders a
    JOIN bigquery-public-data.thelook_ecommerce.order_items b ON a.order_id = b.order_id AND a.user_id = b.user_id
    JOIN bigquery-public-data.thelook_ecommerce.users c ON  a.user_id = c.id
    JOIN bigquery-public-data.thelook_ecommerce.products d ON b.product_id = d.id
    WHERE c.id <> ''
    AND a.num_of_item > 0
),
retail_main AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY order_id, product_id, num_of_item ORDER BY created_at) as STT
        FROM retail
    ) AS t
    WHERE STT =1
), 
retail_index AS (
    SELECT user_id, sale_price, created_at
    FORMAT_DATE('%Y-%m', created_at) AS corhort_date
    ((EXTRACT('year' FROM created_at) - EXTRACT('year' FROM first_purchase_date)) * 12) + ((EXTRACT('month' FROM created_at)) - EXTRACT('month' FROM first_purchase_date) +1) AS index
    FROM(SELECT user_id, sale_price, 
        MIN(created_at) OVER(PARTITION BY user_id) AS first_purchase_date,
        created_at
        FROM retail_main) a
),
XXX AS (
    SELECT cohort_date, index,
    COUNT(DISTINCT user_id) as cnt,
    SUM(sale_price) as revenue
    FROM retail_index
    GROUP BY cohort_date, index
),
--customer_cohort
customer_cohort AS (SELECT 
cohort_date,
SUM(CASE WHEN index = 1 THEN cnt ELSE 0 END) as m1,
SUM(CASE WHEN index = 2 THEN cnt ELSE 0 END) as m2,
SUM(CASE WHEN index = 3 THEN cnt ELSE 0 END) as m3,
SUM(CASE WHEN index = 4 THEN cnt ELSE 0 END) as m4,
SUM(CASE WHEN index = 5 THEN cnt ELSE 0 END) as m5,
SUM(CASE WHEN index = 6 THEN cnt ELSE 0 END) as m6,
SUM(CASE WHEN index = 7 THEN cnt ELSE 0 END) as m7,
SUM(CASE WHEN index = 8 THEN cnt ELSE 0 END) as m8,
SUM(CASE WHEN index = 9 THEN cnt ELSE 0 END) as m9,
SUM(CASE WHEN index = 10 THEN cnt ELSE 0 END) as m10,
SUM(CASE WHEN index = 11 THEN cnt ELSE 0 END) as m11,
SUM(CASE WHEN index = 12 THEN cnt ELSE 0 END) as m12,
SUM(CASE WHEN index = 13 THEN cnt ELSE 0 END) as m13
FROM xxx
GROUP BY cohort_date
ORDER BY cohort_date)
--retention_cohort
SELECT cohort_date,
ROUND(100.00*m1/m1,2) || "%" m1,
ROUND(100.00*m2/m1,2) || "%" m2,
ROUND(100.00*m3/m1,2) || "%" m3,
ROUND(100.00*m4/m1,2) || "%" m4,
ROUND(100.00*m5/m1,2) || "%" m5,
ROUND(100.00*m6/m1,2) || "%" m6,
ROUND(100.00*m7/m1,2) || "%" m7,
ROUND(100.00*m8/m1,2) || "%" m8,
ROUND(100.00*m9/m1,2) || "%" m9,
ROUND(100.00*m10/m1,2) || "%" m10,
ROUND(100.00*m11/m1,2) || "%" m11,
ROUND(100.00*m12/m1,2) || "%" m12,
ROUND(100.00*m13/m1,2) || "%" m13
FROM customer_cohort
--churn_cohort
SELECT cohort_date,
ROUND(100.00-(100.00*m1/m1),2) || "%" m1,
ROUND(100.00-(100.00*m2/m1),2) || "%" m2,
ROUND(100.00-(100.00*m3/m1),2) || "%" m3,
ROUND(100.00-(100.00*m4/m1),2) || "%" m4,
ROUND(100.00-(100.00*m5/m1),2) || "%" m5,
ROUND(100.00-(100.00*m6/m1),2) || "%" m6,
ROUND(100.00-(100.00*m7/m1),2) || "%" m7,
ROUND(100.00-(100.00*m8/m1),2) || "%" m8,
ROUND(100.00-(100.00*m9/m1),2) || "%" m9,
ROUND(100.00-(100.00*m10/m1),2) || "%" m10,
ROUND(100.00-(100.00*m11/m1),2) || "%" m11,
ROUND(100.00-(100.00*m12/m1),2) || "%" m12,
ROUND(100.00-(100.00*m13/m1),2) || "%" m13
FROM customer_cohort
