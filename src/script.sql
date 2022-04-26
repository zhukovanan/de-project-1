--Создаем отношение куда расположим результат RFM-классификации

CREATE TABLE analysis.dm_rfm_segments  (
	user_id int4,
	recency int,
	frequency int,
	monetary_value int,
	CONSTRAINT dm_rfm_segments_recency_check CHECK (((recency >= 1) AND (recency <= 5))),
	CONSTRAINT dm_rfm_segments_frequency_check CHECK (((frequency >= 1) AND (frequency <= 5))),
	CONSTRAINT dm_rfm_segments_monetary_value_check CHECK (((monetary_value >= 1) AND (monetary_value <= 5)))
);

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
INNER JOIN analysis.status as s
ON s.id = orders.status
AND s.key = 'Closed'
AND date_trunc('month', order_ts)::date >= '2021-01-01' 
GROUP BY 
	user_id
)
	
INSERT INTO analysis.dm_rfm_segments
SELECT
	users.id AS user_id,
        ntile(5) over (order by coalesce(last_order_date, to_timestamp(0))) AS recency,
        ntile(5) over (order by coalesce(count_order, 0)) AS frequency,
        ntile(5) over (order by coalesce(value, 0)) AS monetary_value
FROM analysis.users
LEFT JOIN group_mart
ON users.id = group_mart.user_id

```

### Валидация витрины
Запускаем скрипт проверки однородности групп в RFM-сегментах
```
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
```

## Доработка представлений
```
DROP VIEW IF EXISTS analysis.orders;
CREATE VIEW analysis.orders AS (
WITH log as (
SELECT DISTINCT
	order_id,
	last_value(status_id) over (partition by order_id order by dttm) AS status
FROM production.orderstatuslog
)
SELECT 
	o.order_id,
	order_ts,
	user_id,
	bonus_payment,
	payment,
	cost,
	bonus_grant,
	log.status
FROM production.orders as o
INNER JOIN log
ON o.order_id = log.order_id
);
```
