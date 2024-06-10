CREATE OR REPLACE  VIEW analysis.Orders AS
SELECT 
  o.order_id AS order_id,
  o.user_id,
  o.order_ts,
  osl.dttm,
  osl.status_id  AS status
FROM 
  production.Orders o
 LEFT JOIN production.orderstatuslog osl 
    on osl.order_id = o.order_id and 
    osl.dttm = (select max(osl2.dttm) from production.orderstatuslog osl2 where osl2.order_id = o.order_id)
