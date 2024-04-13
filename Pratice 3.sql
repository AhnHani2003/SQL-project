--1) Doanh thu theo từng ProductLine, Year  và DealSize?
SELECT
    productline,
    EXTRACT('year' FROM orderdate),
    dealsize,
    SUM(sales) AS revenue
FROM
    sales_dataset_rfm_prj
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
    sales_dataset_rfm_prj
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
    sales_dataset_rfm_prj
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
FROM sales_dataset_rfm_prj
WHERE country='UK'
GROUP BY YEAR_ID, productline
ORDER BY YEAR_ID)

SELECT YEAR_ID, productline, revenue
FROM UK_REVENUE
WHERE Rank = 1


--5) Ai là khách hàng tốt nhất, phân tích dựa vào RFM 
