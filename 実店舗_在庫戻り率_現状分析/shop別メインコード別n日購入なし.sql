--入荷して14日以上経過したが1つも受注されていない商品ID
create temporary function span_days() as (14);

with 
raw_goods as(
select product_class.id as product_class_id
      ,t_product_id
      ,code as product_code
      ,main_code AS product_main_code
      ,product_name
      ,wholesale_price
      ,retail_price
    from `maxim-office.datalake_lettuce_polydoor.t_product_class` as product_class
    inner join `maxim-office.datalake_lettuce_polydoor.t_product` as product
      on product_class.t_product_id = product.id
    inner join `maxim-office.maxim_product.product` as maxim_product
      on code = product_sku
)
,raw_shop as(
select id as shop_id--pk
      ,name02 as shop_name
    from `maxim-office.datalake_lettuce_polydoor.t_pos_shop` shop
)
,raw_order as(
--実店舗受注情報 2022/01/01~
select _order.id as order_id --pk
      ,od.id as order_detail_id --pk
      ,order_date as order_dt
      ,date(order_date) as order_date
      ,t_product_class_id as product_class_id
      ,od.quantity as order_quantity
      ,od.price
      ,t_pos_shop_id as shop_id
    from `maxim-office.datalake_lettuce_polydoor.t_pos_order` as _order
    inner join `maxim-office.datalake_lettuce_polydoor.t_pos_order_detail` as od
      on _order.id = od.t_pos_order_id
)
,agg_order as(
  select order_date 
        ,shop_id
        ,product_class_id
        ,sum(order_quantity) as order_quantity
      from raw_order
      where order_date between date_add(current_date('Asia/Tokyo'),interval -span_days() -1 day) and date_add(current_date('Asia/Tokyo'),interval -1 day)
  group by all
)
,agg_order_shop_latest as(
  select shop_id
        ,product_class_id
        ,max(order_date) as latest_order_date
      from raw_order
    group by all
)
,raw_shop_arrival as(
--実店舗入荷情報
select arrival.id as arrival_id
      ,arrival_detail.id as arrival_detail_id --pk
      ,t_pos_shop_id as shop_id
      ,t_product_class_id as product_class_id
      ,date(arrival.delivery_date) as delivery_date
      ,date_trunc(date(arrival.delivery_date),month) as delivery_month
      ,arrival_detail.quantity as stock_quantity
    from `maxim-office.datalake_lettuce_polydoor.t_pos_arrival` as arrival
    inner join `maxim-office.datalake_lettuce_polydoor.t_pos_arrival_detail` as arrival_detail
      on arrival.id = arrival_detail.t_pos_arrival_id
  where delivery_date is not null 
      and 0 < quantity --林さんに確認 cancelされていないのに数量が0のデータが112件、
)
,agg_shop_arrival as(
select shop_id
      ,product_class_id
      ,min(delivery_date) as min_delivery_date
      ,max(delivery_date) as max_delivery_date
  from raw_shop_arrival
  group by all
)
,raw_stock as(
select t_pos_shop_id as shop_id
      ,t_product_class_id as product_class_id
      ,sum(stock) as stock_quantity
      ,current_date('Asia/Tokyo') as stock_date
  from `maxim-office.datalake_lettuce_polydoor.t_pos_shop_stock`
  group by all
)
,logic_stock_arrival as(
select raw_shop.shop_id--pk
      ,raw_shop.shop_name
      ,raw_goods.product_class_id --pk
      ,raw_goods.t_product_id
      ,raw_goods.product_code
      ,raw_goods.product_main_code
      ,raw_goods.product_name
      ,raw_goods.wholesale_price
      ,raw_goods.retail_price
      ,raw_stock.stock_quantity
      ,raw_stock.stock_date
      ,agg_order.order_date --pk
      ,agg_order_shop_latest.latest_order_date
      ,ifnull(agg_order.order_quantity,0) as order_quantity
      ,ifnull(agg_shop_arrival.min_delivery_date,"1900-01-01") as min_delivery_date
      ,ifnull(agg_shop_arrival.max_delivery_date,"1900-01-01") as max_delivery_date
      ,if(max_delivery_date <= date_add(stock_date,interval -span_days() -1 day),1,0) as nday_passed_flag
  from raw_stock
  inner join raw_goods using (product_class_id)
  left join raw_shop  using (shop_id)
  left join agg_shop_arrivalhttps://github.com/github-copilot/signup
    on raw_stock.product_class_id = agg_shop_arrival.product_class_id
    and raw_stock.shop_id = agg_shop_arrival.shop_id
  left join agg_order
    on raw_stock.product_class_id = agg_order.product_class_id
    and raw_stock.shop_id = agg_order.shop_id
  left join agg_order_shop_latest
    on raw_stock.product_class_id = agg_order_shop_latest.product_class_id
    and raw_stock.shop_id = agg_order_shop_latest.shop_id
  where 0 < stock_quantity
  -- and (order_date is null 
  --   or order_date between date_add(stock_date,interval -span_days() -1 day) and date_add(stock_date,interval -1 day))
)
,agg_stock_arrival_code as(
select stock_date
      ,shop_id--pk
      ,shop_name
      ,product_class_id --pk
      ,t_product_id
      ,product_code
      ,product_main_code
      ,product_name
      ,latest_order_date
      ,sum(stock_quantity) as stock_quantity
      ,sum(order_quantity) as order_quantity
      ,sum(stock_quantity * wholesale_price) as stock_wholesale_price
      ,sum(order_quantity * wholesale_price) as order_wholesale_price
      ,sum(stock_quantity * retail_price) as stock_retail_price
      ,sum(order_quantity * retail_price) as order_retail_price
      ,min_delivery_date
      ,max_delivery_date
      ,nday_passed_flag
    from logic_stock_arrival
    group by all
)
,agg_stock_arrival_main as(
select stock_date
      ,span_days() as nday
      ,shop_id --pk
      ,shop_name
      ,t_product_id --pk
      ,product_main_code
      ,product_name
      ,sum(stock_quantity) as stock_quantity
      ,sum(order_quantity) as order_quantity
      ,sum(stock_wholesale_price) as stock_wholesale_price
      ,sum(order_wholesale_price) as order_wholesale_price
      ,sum(stock_retail_price) as stock_retail_price
      ,sum(order_retail_price) as order_retail_price
      ,min(min_delivery_date) as min_delivery_date
      ,max(max_delivery_date) as max_delivery_date
      ,max(nday_passed_flag) as nday_passed_flag
      ,max(latest_order_date) as latest_order_date
    from agg_stock_arrival_code
  group by all
  having 0<stock_quantity
)
,logic_stock_arrival_main as
(
select *
      ,if(nday_passed_flag=1 and order_quantity = 0,product_main_code,null) as no_order_product_main_code
      ,if(nday_passed_flag=1 and order_quantity = 0,stock_quantity,null) as no_order_stock_quantity
      ,if(nday_passed_flag=1 and order_quantity = 0,stock_wholesale_price,0) as no_order_stock_wholesale_price
from agg_stock_arrival_main 
)
,main as(
select stock_date
      ,shop_id
      ,shop_name
      ,count(1) as main_code_cnt
      ,sum(stock_quantity) as stock_quantity
      ,floor(sum(stock_wholesale_price)) as stock_wholesale_price
      ,count(distinct(no_order_product_main_code)) as no_order_product_main_code_cnt
      ,sum(no_order_stock_quantity) as no_order_stock_quantity
      ,floor(sum(no_order_stock_wholesale_price)) as no_order_stock_wholesale_price
from logic_stock_arrival_main
group by all
order by stock_date
      ,shop_id
)
select * from main
