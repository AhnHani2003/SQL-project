--1) Doanh thu theo từng ProductLine, Year  và DealSize?
SELECT
    productline,
    EXTRACT('year' FROM orderdate),
    dealsize,
    SUM(sales) AS revenue
FROM
    sales_dataset_rfm_prj_clean
GROUP BY
    productline,
    EXTRACT('year' FROM orderdate),
    dealsize
	
--2) Đâu là tháng có bán tốt nhất mỗi năm?
WITH MonthlyRevenue AS (SELECT
    EXTRACT('year' FROM orderdate) AS YEAR_ID,  EXTRACT('month' FROM orderdate) AS MONTH_ID, 
	SUM(sales) AS REVENUE,
	COUNT(*) AS ORDER_NUMBER,
    ROW_NUMBER() OVER(PARTITION BY EXTRACT('year' FROM orderdate) ORDER BY SUM(sales) DESC) AS Rank
FROM
    sales_dataset_rfm_prj_clean
GROUP BY
    EXTRACT('year' FROM orderdate), EXTRACT('month' FROM orderdate))

SELECT
    MONTH_ID,
	YEAR_ID,
    REVENUE,
    ORDER_NUMBER
FROM
    MonthlyRevenue
WHERE
    Rank = 1;
	
--3) Product line nào được bán nhiều ở tháng 11?
WITH NOVEMBER_ORDER AS(SELECT
    productline,
    EXTRACT('year' FROM orderdate) AS YEAR_ID,  EXTRACT('month' FROM orderdate) AS MONTH_ID, 
    SUM(sales) AS revenue,
	COUNT(*) AS ORDER_NUMBER,
    ROW_NUMBER() OVER(PARTITION BY EXTRACT('year' FROM orderdate), EXTRACT('month' FROM orderdate) ORDER BY COUNT(*) DESC) AS Rank
FROM
    sales_dataset_rfm_prj_clean
GROUP BY
    productline,
    EXTRACT('year' FROM orderdate),  EXTRACT('month' FROM orderdate)
	)

SELECT
    MONTH_ID,
	YEAR_ID,
    REVENUE,
    ORDER_NUMBER,
	Rank
FROM
    NOVEMBER_ORDER
WHERE
    MONTH_ID=11 AND Rank =1
	
	
--4) Đâu là sản phẩm có doanh thu tốt nhất ở UK mỗi năm? 
WITH UK_REVENUE AS (SELECT EXTRACT('year' FROM orderdate) AS YEAR_ID, 
	productline,    
    SUM(sales) AS revenue,
	ROW_NUMBER() OVER(PARTITION BY EXTRACT('year' FROM orderdate) ORDER BY SUM(sales) DESC) AS Rank
FROM sales_dataset_rfm_prj_clean
WHERE country='UK'
GROUP BY YEAR_ID, productline
ORDER BY YEAR_ID)

SELECT YEAR_ID, productline, revenue
FROM UK_REVENUE
WHERE Rank = 1


--5) Ai là khách hàng tốt nhất, phân tích dựa vào RFM 
WITH customer_rfm AS(
SELECT a.customer_id,
CURRENT_DATE - MAX(b.order_date) AS R,
COUNT(DISTINCT b.order_id) AS F,
SUM(b.sales) as M
FROM customer a
JOIN sales b ON a.customer_id = b.customer_id
GROUP BY a.customer_id),
rfm_score AS(
SELECT customer_id,
ntile(5) OVER(ORDER BY R DESC) AS R_score,
ntile(5) OVER(ORDER BY F) AS F_score,
ntile(5) OVER(ORDER BY M) AS M_score
FROM customer_rfm),
rfm_final AS (
SELECT customer_id,
CAST(R_score AS VARCHAR) || CAST(F_score AS VARCHAR) || CAST(M_score AS VARCHAR) AS rfm_score
FROM rfm_score),


/*SELECT segment, COUNT(*)
FROM(SELECT customer_id, b.segment FROM
rfm_final a
JOIN segment_score b ON a.rfm_score = b.scores) AS a
GROUP BY segment
ORDER BY COUNT (*)*/

customer_rank AS(
SELECT customer_id, CAST(rfm_score AS numeric) AS numeric_score,
DENSE_RANK() OVER(ORDER BY  CAST(rfm_score AS numeric)DESC) AS RANK
FROM rfm_final
ORDER BY numeric_score DESC)

SELECT customer_id, numeric_score FROM customer_rank
WHERE RANK = 1
