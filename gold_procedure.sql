IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold;');
GO

-- Drop existing view if exists
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

---Creating views for  Gold layer 
-- Checking duplicates after joining tables
-- Renaming columns
select customer_id, count(*) from
(
select
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as firstname,
    ci.cst_lastname as lastname,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
    else coalesce(ca.gen,'n/a')
    end as gender,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid)t
group by customer_id
having count(*) > 1

-- Data integration 
select distinct 
    ci.cst_gndr,
    ca.gen,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
    else coalesce(ca.gen,'n/a')
    end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid;

 -- Creating dim_customers view
create view gold.dim_customers as 
select
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as firstname,
    ci.cst_lastname as lastname,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    case when ci.cst_gndr != 'n/a' then ci.cst_gndr
    else coalesce(ca.gen,'n/a')
    end as gender,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid;

--checking duplicates after joining tables
select prd_key, count(*) from (
select 
      pn.prd_id,
	  pn.cat_id,
	  pn.prd_key,
	  pn.prd_nm,
	  pn.prd_cost,
	  pn.prd_line,
	  pn.prd_start_dt,
      pn.prd_end_dt,
	  pc.cat,
      pc.subcat,
      pc.maintenance
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt is null
)t group by prd_key
having count(*) > 1
-- filter out historical data

-- Creating dim_products view
create view gold.dim_products as 
select 
      row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
      pn.prd_id as product_id,
      pn.prd_key as product_number,
      pn.prd_nm as product_name,
	  pn.cat_id as category_id,
      pc.cat as category,
      pc.subcat as subcategory,
      pc.maintenance,
      pn.prd_cost as cost,
	  pn.prd_line as product_line,
	  pn.prd_start_dt as start_dt 
from silver.crm_prd_info as pn
left join silver.erp_px_cat_g1v2 as pc
on pn.cat_id = pc.id
where prd_end_dt is null


-- Creating Fact view
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

select * from gold.dim_customers

select * from  gold.dim_products

select * from  gold.fact_sales



-- checking foreign key integrity
select *
from gold.fact_sales f
left join gold.dim_customers c 
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null or c.customer_key is null

drop view gold.dim_customers

exec load_gold;

CREATE OR ALTER PROCEDURE load_gold AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @batch_start_time DATETIME, 
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Gold Layer';
        PRINT '================================================';

        -- Drop views if they exist
        IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL DROP VIEW gold.dim_customers;
        IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL DROP VIEW gold.dim_products;
        IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL DROP VIEW gold.fact_sales;

        -------------------------------
        -- Create gold.dim_customers
        -------------------------------
        EXEC('
        create view gold.dim_customers as 
        select
            row_number() over (order by cst_id) as customer_key,
            ci.cst_id as customer_id,
            ci.cst_key as customer_number,
            ci.cst_firstname as firstname,
            ci.cst_lastname as lastname,
            la.cntry as country,
            ci.cst_marital_status as marital_status,
            case when ci.cst_gndr != ''n/a'' then ci.cst_gndr
            else coalesce(ca.gen,''n/a'')
            end as gender,
            ca.bdate as birthdate,
            ci.cst_create_date as create_date
        from silver.crm_cust_info ci
        left join silver.erp_cust_az12 ca
        on ci.cst_key = ca.cid
        left join silver.erp_loc_a101 la
        on ci.cst_key=la.cid
        ');

        -------------------------------
        -- Create gold.dim_products
        -------------------------------
        EXEC('
        create view gold.dim_products as 
        select 
              row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
              pn.prd_id as product_id,
              pn.prd_key as product_number,
              pn.prd_nm as product_name,
	          pn.cat_id as category_id,
              pc.cat as category,
              pc.subcat as subcategory,
              pc.maintenance,
              pn.prd_cost as cost,
	          pn.prd_line as product_line,
	          pn.prd_start_dt as start_dt 
        from silver.crm_prd_info as pn
        left join silver.erp_px_cat_g1v2 as pc
        on pn.cat_id = pc.id
        where prd_end_dt is null

        ');

        -------------------------------
        -- Create gold.fact_sales
        -------------------------------
        EXEC('
        CREATE VIEW gold.fact_sales AS
        SELECT
            sd.sls_ord_num  AS order_number,
            pr.product_key  AS product_key,
            cu.customer_key AS customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt  AS shipping_date,
            sd.sls_due_dt   AS due_date,
            sd.sls_sales    AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price    AS price
        FROM silver.crm_sales_details sd
        LEFT JOIN gold.dim_products pr
            ON sd.sls_prd_key = pr.product_number
        LEFT JOIN gold.dim_customers cu
            ON sd.sls_cust_id = cu.customer_id'
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
        PRINT 'Gold Layer Loading Completed Successfully';
        PRINT '==========================================';
    END TRY

    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING GOLD LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO
