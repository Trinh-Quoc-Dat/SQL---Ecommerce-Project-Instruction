--Lưu ý chung: với Bigquery thì mình có thể group by, order by 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Mình k nên xử lý date bằng những hàm đc dùng để xử lý chuỗi như left, substring, concat
--vì lúc này data của mình vẫn ở dạng string, chứ k phải dạng date, khi xuất ra excel hay gg sheet thì phải xử lý thêm 1 bước nữa
--k nên đặt tên CTE là cte hoặc ABC,nên đặt tên viết tắt, mà nhìn vào mình có thể hiểu đc CTE đó đang lấy data gì

--1
SELECT format_date("%Y%m", parse_date("%Y%m%d", date)) as month,  -- left(date,6) as month,
      sum(totals.visits) as visits,
      sum(totals.pageviews) as pageviews,
      sum(totals.transactions) as transactions
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
--where left(date,6) IN ('201701','201702','201703')
WHERE _TABLE_SUFFIX BETWEEN '0101' AND '0331'
group by month
order by month
--2
with t as (
      SELECT 
            trafficSource.source as source,
            SUM(totals.visits) as total_visits,
            SUM(totals.bounces) as total_no_of_bounces,
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      GROUP BY source
      ORDER BY total_visits DESC
)
SELECT 
      source,
      total_visits,
      total_no_of_bounces,
      (total_no_of_bounces/total_visits*100) as bounce_rate
FROM t
--mình có thể ghi ngắn gọn lại như thế này
SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;

--3
WITH get_revenue_month AS (
      SELECT
            "Month" AS time_type,
            format_date("%Y%m", parse_date("%Y%m%d", date)) AS time,
            trafficSource.source AS source,
            SUM(totals.totalTransactionRevenue)/1000000 AS revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      WHERE _table_suffix between '0601' and '0630'
      GROUP BY 1,2,3),

get_revenue_week AS (
      SELECT
            "Week" AS time_type,
            format_date("%Y%W", parse_date("%Y%m%d", date)) AS time,
            trafficSource.source AS source,
            SUM(totals.totalTransactionRevenue)/1000000 AS revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      WHERE _table_suffix BETWEEN '0601' AND '0630'
      GROUP BY 1,2,3)

SELECT * FROM get_revenue_month
UNION ALL 
SELECT * FROM get_revenue_week
ORDER BY revenue DESC
--correct

--4
WITH month_6 AS (
      SELECT  
            "201706" AS MONTH,
            SUM(CASE WHEN totals.transactions >=1 THEN totals.pageviews END) AS TOTAL_PUR_PAGEVIEWS,
            SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews END) AS TOTAL_NON_PUR_PAGEVIEWS,
            COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END)) AS NUM_PUR,
            COUNT(DISTINCT(CASE WHEN totals.transactions IS NULL THEN fullVisitorId END)) AS NUM_NON_PUR
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`),

month_7 AS (
      SELECT  
            "201707" AS MONTH,
            SUM(CASE WHEN totals.transactions >=1 THEN totals.pageviews END) AS TOTAL_PUR_PAGEVIEWS,
            SUM(CASE WHEN totals.transactions IS NULL THEN totals.pageviews END) AS TOTAL_NON_PUR_PAGEVIEWS,
            COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END)) AS NUM_PUR,
            COUNT(DISTINCT(CASE WHEN totals.transactions IS NULL THEN fullVisitorId END)) AS NUM_NON_PUR
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`)

SELECT MONTH as month ,
TOTAL_PUR_PAGEVIEWS/NUM_PUR as avg_pageviews_purchase,
TOTAL_NON_PUR_PAGEVIEWS/NUM_NON_PUR as avg_pageviews_non_purchase
FROM month_6

UNION ALL

SELECT MONTH as month ,
TOTAL_PUR_PAGEVIEWS/NUM_PUR as avg_pageviews_purchase,
TOTAL_NON_PUR_PAGEVIEWS/NUM_NON_PUR as avg_pageviews_non_purchase
FROM month_7
ORDER BY MONTH;
/*
mình k nên break cte theo tháng, vì nếu data lấy 12 tháng, mình k thể ghi 12 cte đc, chứ select month ra, khi mình dùng
aggregate function, nó sẽ tự group theo từng tháng
thứ 2 là mình nên break cte dựa trên yêu cầu của bài toán, ở đâu là sự khác nhau giữa purchaser và non-purchasers, mình nên break làm 2
*/

with 
purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
    ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  and product.productRevenue is not null
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
      ,unnest(hits) hits
    ,unnest(product) product
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  and product.productRevenue is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;


--câu 4 này lưu ý là mình nên dùng left join hoặc full join, bởi vì trong câu này, phạm vi chỉ từ tháng 6-7, nên chắc chắc sẽ có pur và nonpur của cả 2 tháng
--mình inner join thì vô tình nó sẽ ra đúng. nhưng nếu đề bài là 1 khoảng thời gian dài hơn, 2-3 năm chẳng hạn, nó cũng tháng chỉ có nonpur mà k có pur
--thì khi đó inner join nó sẽ làm mình bị mất data, thay vì hiện số của nonpur và pur thì nó để trống


--5
WITH GET_AVG_7_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201707" END AS Month,
--xem lại cách xử lý date, cần có cách xử lý hay hơn, nếu print 201707 thì chỉ cần ghi '201707' as month, chứ k cần ghi case when 1=1 then
SUM(CASE WHEN totals.transactions >=1 THEN totals.transactions END ) AS total_transactions,
COUNT(DISTINCT(CASE WHEN totals.transactions >=1 THEN fullVisitorId END )) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`)

SELECT 
Month,
total_transactions/NUM_USER as Avg_total_transactions_per_user
FROM GET_AVG_7_MONTH
--mình chỉ cần filter ra purchase, rồi tính toán tiếp, chứ k cần ghi case when phức tạp
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    ,unnest (hits) hits,
    unnest(product) product
where  totals.transactions>=1
and totals.totalTransactionRevenue is not null
and product.productRevenue is not null
group by month;


--6
WITH GET_AVG_7_MONTH AS (SELECT
CASE WHEN 1 = 1 THEN "201707" END AS Month,
SUM(CASE WHEN 1 = 1 THEN totals.totalTransactionRevenue END ) AS total_trans_revenue,
COUNT(CASE WHEN 1 = 1 THEN fullVisitorId  END ) AS NUM_USER
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL )

SELECT 
Month,
format("%'.2f",total_trans_revenue/NUM_USER) as Avg_total_transactions_per_user
FROM GET_AVG_7_MONTH

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(product.productRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,unnest(hits) hits
  ,unnest(product) product
where product.productRevenue is not null
group by month;



-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce
#standardSQL

/*
yêu cầu của bài là tìm những sản phẩm được mua bởi những khách hàng mà họ đã từng mua sản phẩm A
step1: tìm list customers(1) đã mua sản phẩm A
step2: tìm ra những sản phẩm được mua bởi (1)
*/
--khi lấy điều kiện chính xác như 'Youtube....' thì nên dùng dấu = hoặc in hoặc <>, k nên dùng like, thì like thường dùng để tìm kiếm gần giống

--subquery:
select
    product.v2productname as other_purchased_product,
    sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
where fullvisitorid in (select distinct fullvisitorid
                        from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                        unnest(hits) as hits,
                        unnest(hits.product) as product
                        where product.v2productname = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
and product.v2productname != "YouTube Men's Vintage Henley"
and product.productRevenue is not null
group by other_purchased_product
order by quantity desc;

--CTE:

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC;



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

/* Với mỗi sản phẩm, nó sẽ trải qua 3 stage, view -> add to cart -> purchase
thì để bài đang yêu cầu mình tính theo kiểu cohort map, qua từng stage như vậy, số sản phầm rớt dần còn bao nhiêu %
ví dụ có mình xem 10 sản phẩm, xong bỏ 4 sản phẩm vào giỏ hàng, rồi quyết định chỉ mua 1 cái thôi
*/

--bài yêu cầu tính số sản phầm, mình nên count productName hay productSKU thì sẽ hợp lý hơn là count action_type
--k nên xài inner join, nếu table1 có 10 record,table2 có 5 record,table3 có 1 record, thì sau khi inner join, output chỉ ra 1 record

--Cách 1:dùng CTE
with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
and product.productRevenue is not null   --phải thêm điều kiện này để đảm bảo có revenue
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
left join add_to_cart a on pv.month = a.month
left join purchase p on pv.month = p.month
order by pv.month;

--bài này k nên inner join, vì nếu như bảng purchase k có data thì sẽ k mapping đc vs bảng productview, từ đó kết quả sẽ k có luôn, mình nên dùng left join

--Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data;
