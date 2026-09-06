

CREATE TABLE customers (
    customer_id   INT PRIMARY KEY,
    customer_name VARCHAR(100),
    city          VARCHAR(100),
    updated_at    TIMESTAMP
);

CREATE TABLE stg_customers (
    customer_id   INT,
    customer_name VARCHAR(100),
    city          VARCHAR(100),
    updated_at    TIMESTAMP
);



SELECT * FROM customers;

delete from customers

CREATE TABLE stg_customers (
	customer_id INT, 
	customer_name VARCHAR(100),
	city VARCHAR(100),
	updated_at TIMESTAMP
)

SELECT * FROM stg_customers;
SELECT * FROM customers;

MERGE INTO customers t
USING stg_customers s
ON t.customer_id = s.customer_id

WHEN MATCHED THEN
UPDATE SET
	customer_name = s.customer_name,
	city = s.city,
	updated_at = s.updated_at
	
WHEN NOT MATCHED THEN
INSERT (
	customer_id,
	customer_name,
	city,
	updated_at
)
VALUES (
	s.customer_id,
	s.customer_name,
	s.city,
	s.updated_at
);

SELECT version();

