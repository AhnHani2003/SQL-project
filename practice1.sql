create table SALES_DATASET_RFM_PRJ
(
  ordernumber VARCHAR,
  quantityordered VARCHAR,
  priceeach        VARCHAR,
  orderlinenumber  VARCHAR,
  sales            VARCHAR,
  orderdate        VARCHAR,
  status           VARCHAR,
  productline      VARCHAR,
  msrp             VARCHAR,
  productcode      VARCHAR,
  customername     VARCHAR,
  phone            VARCHAR,
  addressline1     VARCHAR,
  addressline2     VARCHAR,
  city             VARCHAR,
  state            VARCHAR,
  postalcode       VARCHAR,
  country          VARCHAR,
  territory        VARCHAR,
  contactfullname  VARCHAR,
  dealsize         VARCHAR
) 
--Chuyển đổi kiểu dữ liệu phù hợp cho các trường ( sử dụng câu lệnh ALTER) 
ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN priceeach TYPE numeric USING (trim(priceeach)::numeric);

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN ordernumber TYPE numeric USING (trim(ordernumber)::numeric);

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN quantityordered TYPE numeric USING (trim(quantityordered)::numeric);

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN orderlinenumber TYPE numeric USING (trim(orderlinenumber)::numeric);

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN sales TYPE numeric USING (trim(sales)::numeric);

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN orderdate TYPE TIMESTAMP USING (to_timestamp(orderdate, 'MM/DD/YYYY HH24:MI'));

ALTER TABLE SALES_DATASET_RFM_PRJ
ALTER COLUMN msrp TYPE numeric USING (trim(msrp)::numeric);
--Check NULL/BLANK (‘’)  ở các trường: ORDERNUMBER, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER, SALES, ORDERDATE.
-- Check for NULL or empty strings in the ORDERNUMBER column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE ORDERNUMBER IS NULL OR ORDERNUMB   ER = '';

-- Check for NULL or '' in the QUANTITYORDERED column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE QUANTITYORDERED IS NULL OR QUANTITYORDERED = '';

-- Check for NULL or '' in the PRICEEACH column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE PRICEEACH IS NULL OR PRICEEACH = '';

-- Check for NULL or '' in the ORDERLINENUMBER column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE ORDERLINENUMBER IS NULL OR ORDERLINENUMBER = '';

-- Check for NULL or '' in the SALES column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE SALES IS NULL OR SALES = '';

-- Check for NULL or '' in the ORDERDATE column
SELECT *
FROM SALES_DATASET_RFM_PRJ
WHERE ORDERDATE IS NULL OR ORDERDATE = '';
