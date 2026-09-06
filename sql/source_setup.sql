


-- Create table in src_db

CREATE TABLE customers (
	customer_id INT PRIMARY KEY,
	customer_name VARCHAR(100),
	city VARCHAR(100),
	updated_at TIMESTAMP
);

INSERT INTO customers
VALUES
(1, 'NISH')

USE DATABASE target_db;

CREATE TABLE customers (
	customer_id INT PRIMARY KEY,
	customer_name VARCHAR(100),
	city VARCHAR(100),
	updated_at TIMESTAMP
);


INSERT INTO customers VALUES
(1, 'Nishant', 'Bengaluru', '2026-08-28 09:00:00'),
(2, 'Abhay',   'Chennai',   '2026-08-28 09:05:00'),
(3, 'Yasir',   'Noida',     '2026-08-28 09:10:00');

select * from customers

UPDATE customers
SET city = 'Bengaluru',
    updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 2


INSERT INTO customers
VALUES (
    4,
    'Prakash',
    'Bengaluru',
    CURRENT_TIMESTAMP
);
