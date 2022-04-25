--Создаем отношение куда расположим результат RFM-классификации

CREATE TABLE analysis.dm_rfm_segments  (
	user_id int4,
	recency int,
	frequency int,
	monetary_value int
	
)

--Представление для заказов из схемы production
CREATE VIEW analysis.orders AS (
SELECT 
	order_id,
	order_ts,
	user_id,
	bonus_payment,
	payment,
	cost,
	bonus_grant,
	status
FROM production.orders
);

--Представление для статусов заказов из схемы production
CREATE VIEW analysis.status AS (
SELECT 
	id,
	key
FROM production.orderstatuses 
);

--Представление для пользователей из схемы production
CREATE VIEW analysis.users AS (
SELECT 
	id,
	name,
	login
FROM production.users
);

--Представление для товаров из схемы production
CREATE VIEW analysis.products AS (
SELECT 
	id,
	name,
	price
FROM production.products
);

--Представление для состава заказов из схемы production
CREATE VIEW analysis.orderitems AS (
SELECT 
	id,
	product_id,
	order_id,
	name,
	price,
	discount,
	quantity
FROM production.orderitems 
);

--RFM-классификация клиентской базы и наполнение витрины
WITH group_mart AS 
(
SELECT
	user_id,
	max(order_ts) AS last_order_date,
	count(*) AS count_order,
	sum(payment) AS value
FROM analysis.orders
WHERE status = 4 
AND date_trunc('month', order_ts)::date >= '2021-01-01' 
GROUP BY 
	user_id
)
	
INSERT INTO analysis.dm_rfm_segments
SELECT
	users.id AS user_id,
        ntile(5) over (order by coalesce(last_order_date,'1932-02-23 08:07:08.000')) AS recency,
        ntile(5) over (order by coalesce(count_order,0)) AS frequency,
        ntile(5) over (order by coalesce(value,0)) AS monetary_value
FROM analysis.users
LEFT JOIN group_mart
ON users.id = group_mart.user_id



--Валидация однородности сегментов по recency
SELECT 
	recency,
	count(*)
FROM analysis.dm_rfm_segments
GROUP BY 
	recency
HAVING
	count(*) != (SELECT COUNT(*)/5 FROM analysis.users)

--Валидация однородности сегментов по frequency
SELECT
	frequency,
	count(*)
FROM analysis.dm_rfm_segments
GROUP BY 
	frequency
HAVING 
	count(*) != (SELECT COUNT(*)/5 FROM analysis.users)

--Валидация однородности сегментов по monetary_value
SELECT 
	monetary_value,
	count(*)
FROM analysis.dm_rfm_segments
GROUP BY 
	monetary_value
HAVING 
	count(*) != (SELECT COUNT(*)/5 FROM analysis.users)

DROP VIEW IF EXISTS analysis.orders;
CREATE VIEW analysis.orders AS (

SELECT 
	o.order_id,
	order_ts,
	user_id,
	bonus_payment,
	payment,
	cost,
	bonus_grant,
	status
FROM production.orders as o
INNER JOIN production.orderstatuslog AS log
ON o.order_id = log.order_id
WHERE log.dttm = (
SELECT MAX(dttm)
FROM production.orderstatuslog
WHERE o.order_id = orderstatuslog.order_id)
);
