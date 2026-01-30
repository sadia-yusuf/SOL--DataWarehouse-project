
CREATE DATABASE data_warehouse;

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
cst_id	INT,
cst_key	VARCHAR(20),
cst_firstname	VARCHAR(50),
cst_lastname	VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gndr	VARCHAR(5),
cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);


CALL BRONZE.LOAD_BRONZE();

CREATE OR REPLACE PROCEDURE BRONZE.LOAD_BRONZE()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows        INT;
    v_start_time  TIMESTAMP;
    v_end_time    TIMESTAMP;
    v_table_time  INTERVAL;
    v_total_start TIMESTAMP;
    v_total_end   TIMESTAMP;
BEGIN

 -- ==============================
    -- START TOTAL TIMER
    -- ==============================
v_total_start := clock_timestamp();
RAISE NOTICE 'Bronze load started at %', v_total_start;
	
RAISE NOTICE '================================';
RAISE NOTICE 'Bronze layer loaded successfully';
RAISE NOTICE '================================';

RAISE NOTICE '---------------------------------';
RAISE NOTICE  'CRM TABLES LOADED SUCCESSFULLY';
RAISE NOTICE '---------------------------------';

 v_start_time := clock_timestamp();

TRUNCATE TABLE bronze.crm_cust_info;

RAISE NOTICE '-----INSERTING DATA INTO CRM_CUS_INFO------';

COPY bronze.crm_cust_info 
FROM  'C:\Users\Public\Documents\source_crm\cust_info.csv'
WITH DELIMITER ','
     CSV HEADER;

GET DIAGNOSTICS v_rows = ROW_COUNT;
    v_end_time := clock_timestamp();
    v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'crm_cust_info | rows: % | time: %', v_rows, v_table_time;

TRUNCATE TABLE bronze.crm_prd_info;

RAISE NOTICE '-----INSERTING DATA INTO CRM_PRD_INFO------';
 v_start_time := clock_timestamp();

COPY bronze.crm_prd_info
FROM  'C:\Users\Public\Documents\source_crm\prd_info.csv'
WITH DELIMITER ','
     CSV HEADER;

GET DIAGNOSTICS v_rows = ROW_COUNT;
 v_end_time := clock_timestamp();
 v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'crm_prd_info  | rows: % | time: %', v_rows, v_table_time;

TRUNCATE TABLE bronze.crm_sales_details;

RAISE NOTICE '-----INSERTING DATA INTO CRM_SALES-DETAILS------';
 v_start_time := clock_timestamp();


COPY bronze.crm_sales_details
FROM  'C:\Users\Public\Documents\source_crm\sales_details.csv'
WITH DELIMITER ','
     CSV HEADER;

GET DIAGNOSTICS v_rows = ROW_COUNT;
 v_end_time := clock_timestamp();
 v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'crm_sales_details | rows: % | time: %', v_rows, v_table_time;


RAISE NOTICE '---------------------------------';
RAISE NOTICE  'ERP TABLES LOADED SUCCESSFULLY';
RAISE NOTICE '---------------------------------';

TRUNCATE TABLE bronze.erp_cust_az12;

RAISE NOTICE '-----INSERTING DATA INTO ERP_CUST_AZ12-----';
 v_start_time := clock_timestamp();
 
COPY bronze.erp_cust_az12
FROM  'C:\Users\Public\Documents\source_erp\CUST_AZ12.csv'
WITH DELIMITER ','
     CSV HEADER;

GET DIAGNOSTICS v_rows = ROW_COUNT;
 v_end_time := clock_timestamp();
 v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'erp_cust_az12 | rows: % | time: %', v_rows, v_table_time;

TRUNCATE TABLE bronze.erp_loc_a101;

RAISE NOTICE '-----INSERTING DATA INTO ERP_loc_a101-----';
 v_start_time := clock_timestamp();

 
COPY bronze.erp_loc_a101
FROM  'C:\Users\Public\Documents\source_erp\loc_a101.csv'
WITH DELIMITER ','
     CSV HEADER;

GET DIAGNOSTICS v_rows = ROW_COUNT;
 v_end_time := clock_timestamp();
 v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'erp_loc_a101 | rows: % | time: %', v_rows, v_table_time;


TRUNCATE TABLE bronze.erp_px_cat_g1v2;

RAISE NOTICE '-----INSERTING DATA INTO ERP_px_cat_g1v2---';
 v_start_time := clock_timestamp();
 
COPY bronze.erp_px_cat_g1v2
FROM  'C:\Users\Public\Documents\source_erp\px_cat_g1v2.csv'
WITH DELIMITER ','
     CSV HEADER;
GET DIAGNOSTICS v_rows = ROW_COUNT;
 v_end_time := clock_timestamp();
 v_table_time := v_end_time - v_start_time;
 RAISE NOTICE 'erp_px_cat_g1v2| rows: % | time: %', v_rows, v_table_time;

 -- ==============================
    -- END TOTAL TIMER
    -- ==============================
    v_total_end := clock_timestamp();

    RAISE NOTICE '------------------------------------------';
    RAISE NOTICE 'Bronze load completed';
    RAISE NOTICE 'Total execution time: %', v_total_end - v_total_start;
    RAISE NOTICE '------------------------------------------';
 
END
$$;












