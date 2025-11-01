--- data cleaning and transformation
use master
select * from silver.erp_cust_az12;
 
-- checking duplicate values in primary key(cst_id)
select id,count(*) 
from bronze.erp_px_cat_g1v2
group by id
having count(*) >1 or id is null

-- checking null
select *
from bronze.erp_px_cat_g1v2
where  is null;

-- checking unwanted spaces
select prd_nm 
from bronze.crm_prd_info
where prd_nm != trim(prd_nm) ;

-- cheking how standardized the data is
select distinct prd_line
from bronze.crm_sales_details ;

-- inserting data in crm_cust_info 
insert into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date 
)
select 
	cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,
    case when upper(trim(cst_marital_status))='S' then 'Single'
         when upper(trim(cst_marital_status))='M' then 'Married'
         else 'n/a'
    END cst_marital_status,
    case when upper(trim(cst_gndr))='F' then 'Female'
         when upper(trim(cst_gndr))='M' then 'Male'
         else 'n/a'
    END cst_gndr,
    cst_create_date
from
( select *,
row_number() over(partition by cst_id order by cst_create_date desc) as flag
from bronze.crm_cust_info
) as cte
where flag = 1 ;

-- CHECK NULLS AND DUPLICATES IN PRIMARY KEY
select prd_id, count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null;

-- CHECK NULL OR  NEGATIVE NUMBERS
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null;


-- DATA STANDARDIZATION  AND CONSISTENCY
select distinct prd_line
from bronze.crm_prd_info;


--- CHECK INVALID DATE ORDER
select *
from bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
  

 
select * from bronze.erp_loc_a101;


DELETE  
from bronze.crm_cust_info
where cst_id is null;


--- insert data in crm_prd_info
INSERT INTO  silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
    prd_id,
    replace(substring(prd_key,1, 5),'-','_') as cat_id,
    substring(prd_key,7,LEN(prd_key)) as prd_key_new,
    prd_nm,
    isnull(prd_cost,0) as prd_cost,
    case upper(trim(prd_line)) 
    when 'M' THEN 'Mountain'
    when 'R' THEN 'Road'
    when 'S' THEN 'Other Sales'
    when 'T' THEN 'Touring'
    ELSE 'n/a'
    end as prd_line,
    cast(prd_start_dt as date) as prd_start_dt,
    cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt_test
from bronze.crm_prd_info;


-- check for invalid dates and date datatype converstion
select
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt<=0
or len(sls_order_dt) !=8
or sls_order_dt > 20500101
or sls_order_dt < 19000101
;

-- check invalid order date
select
* 
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt>sls_due_dt;


-- check data consistency between sales, quantity and price
-- sales == quantity * price
-- values must not be null, zero or negative
select distinct
    sls_sales,
    sls_quantity,
    sls_price,
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price)
    then sls_quantity * abs(sls_price)
    else sls_sales
    end as sls_sales_new,
    case when sls_price is null or sls_price <= 0
    then sls_sales / nullif(sls_quantity,0)
    else sls_price
    end as sls_price_new
from bronze.crm_sales_details
where sls_sales != sls_quantity*sls_price
; 


--- insert data in crm_sales_details
insert into silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
select  
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
    else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
    else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
    else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt,
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price)
    then sls_quantity * abs(sls_price)
    else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <= 0
    then sls_sales / nullif(sls_quantity,0)
    else sls_price
    end as sls_price
from bronze.crm_sales_details
;


-- check date
select distinct bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- data standardization and consistency
select distinct gen,
case when upper(trim(gen)) in ('F','Female') then 'Female'
when upper(trim(gen)) in ('M','Male') then 'Male'
else 'n/a'  
end as gen_new
from bronze.erp_cust_az12;


--- insert data in erp_cust_az12
insert into silver.erp_cust_az12(
cid,
bdate,
gen
)
select 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,len(cid))
ELSE cid
end as cid,
case when bdate > getdate() then null
else bdate
end as bdate,
case when upper(trim(gen)) in ('F','Female') then 'Female'
when upper(trim(gen)) in ('M','Male') then 'Male'
else 'n/a'  
end as gen
from bronze.erp_cust_az12 
;

-- data standardization  
select
replace(cid,'-','') as cid,
cntry 
from bronze.erp_loc_a101
where replace(cid,'-','') not in
(select cst_key from silver.crm_cust_info);


-- data standardization and consistency
select distinct cntry
from silver.erp_loc_a101
order by cntry;

--- insert data in erp_loc_a101
insert into silver.erp_loc_a101(cid,cntry)
select
replace(cid,'-','') as cid,
case when trim(cntry) = 'DE' THEN 'Germany'
when trim(cntry) IN ('US','USA') THEN 'United States'
when trim(cntry) = '' or cntry is null then 'n/a'
else trim(cntry)
end as cntry
from bronze.erp_loc_a101
;
 
-- check for unwanted spaces
select *
from bronze.erp_px_cat_g1v2
where cat != trim(cat) 
or  subcat != trim(subcat)
or  maintenance != trim(maintenance);


-- data standardization and consistency
select distinct 
cat,
subcat, 
maintenance
from bronze.erp_px_cat_g1v2;

--- insert data in erp_px_cat_g1v2
insert into silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintenance)
select 
    id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;

select * from silver.crm_cust_info

 


