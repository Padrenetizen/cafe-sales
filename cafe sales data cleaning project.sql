USE cafe_sales;

CREATE TABLE cafe_staging
LIKE dirty_cafe_sales;

INSERT INTO cafe_staging
SELECT *
FROM dirty_cafe_sales;

SELECT *
FROM cafe_staging;


SELECT *,
ROW_NUMBER() OVER(
PARTITION BY item, quantity, total_spent, transaction_date) AS row_num
FROM cafe_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY item, quantity, total_spent, transaction_date) AS row_num
FROM cafe_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM cafe_staging
WHERE transaction_date = '2023-05-16';

WITH duplicate_cte2 AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY item, quantity, total_spent,
 transaction_date, price_per_unit, payment_method, location) AS row_num
FROM cafe_staging
)
SELECT *
FROM duplicate_cte2
WHERE row_num > 1;

CREATE TABLE `cafe_staging2` (
  `transaction_id` text,
  `item` text,
  `quantity` int DEFAULT NULL,
  `price_per_unit` double DEFAULT NULL,
  `total_spent` text,
  `payment_method` text,
  `location` text,
  `transaction_date` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO cafe_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY item, quantity, total_spent,
 transaction_date, price_per_unit, payment_method, location) AS row_num
FROM cafe_staging;

SELECT *
FROM cafe_staging2
WHERE row_num > 1;

DELETE
FROM cafe_staging2
WHERE row_num > 1;

SELECT *
FROM cafe_staging2;

SELECT *
FROM cafe_staging2
WHERE transaction_date = 'ERROR';

UPDATE cafe_staging2
SET transaction_date = '2024-06-07'
WHERE transaction_date = 'ERROR';

SELECT transaction_date,
STR_TO_DATE(transaction_date, '%Y-%m-%d')
FROM cafe_staging2;

UPDATE cafe_staging2
SET transaction_date = STR_TO_DATE(transaction_date, '%Y-%m-%d');

SELECT *
FROM cafe_staging2
WHERE transaction_date = '';

-- null values
SELECT *
FROM cafe_staging2
WHERE transaction_date IS NULL OR transaction_date = '';

SELECT *
FROM cafe_staging2
WHERE transaction_date IS NULL;

SELECT *
FROM cafe_staging2
WHERE item IS NULL OR item = '';

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL OR total_spent = '';

SELECT *
FROM cafe_staging2
WHERE payment_method IS NULL OR payment_method = '';

SELECT *
FROM cafe_staging2
WHERE location IS NULL OR location = '';

-- updating blank rows
UPDATE cafe_staging2
SET transaction_date = NULL
WHERE transaction_date = '';

UPDATE cafe_staging2
SET item = NULL
WHERE item = '';

UPDATE cafe_staging2
SET total_spent = NULL
WHERE total_spent = '';

UPDATE cafe_staging2
SET payment_method = NULL
WHERE payment_method = '';

UPDATE cafe_staging2
SET location = NULL
WHERE location = '';

-- checking for irregulars
SELECT DISTINCT transaction_date
FROM cafe_staging2;

SELECT *
FROM cafe_staging2
WHERE transaction_date = 'UNKNOWN';

-- updating irregulars
UPDATE cafe_staging2
SET transaction_date = '2024-12-01'
WHERE transaction_date = 'UNKNOWN';

UPDATE cafe_staging2
SET transaction_date = '2025-06-27'
WHERE transaction_date IS NULL;

SELECT *
FROM cafe_staging2;

SELECT transaction_date,
STR_TO_DATE(transaction_date, '%Y-%m-%d')
FROM cafe_staging2;

UPDATE cafe_staging2
SET transaction_date = STR_TO_DATE(transaction_date, '%Y-%m-%d');

SELECT *
FROM cafe_staging2;

-- updating the item column
SELECT *
FROM cafe_staging2
WHERE item IS NULL AND quantity = 1;

UPDATE cafe_staging2
SET item = 'Macaroon'
WHERE item IS NULL AND quantity = 1;

SELECT *
FROM cafe_staging2
WHERE quantity = 1;

SELECT *
FROM cafe_staging2
WHERE item IS NULL AND quantity = 2;

UPDATE cafe_staging2
SET item = 'Bbq Ribs'
WHERE item IS NULL AND quantity = 2;

SELECT *
FROM cafe_staging2
WHERE quantity = 2;

SELECT *
FROM cafe_staging2
WHERE item IS NULL AND quantity = 3;

UPDATE cafe_staging2
SET item = 'Caviar'
WHERE item IS NULL AND quantity = 3;

SELECT *
FROM cafe_staging2
WHERE quantity = 3;

SELECT *
FROM cafe_staging2
WHERE item IS NULL AND quantity = 4;

UPDATE cafe_staging2
SET item = 'Short Ribs'
WHERE item IS NULL AND quantity = 4;

SELECT *
FROM cafe_staging2
WHERE quantity = 4;

SELECT *
FROM cafe_staging2
WHERE item IS NULL AND quantity = 5;

UPDATE cafe_staging2
SET item = 'Chinese fried rice'
WHERE item IS NULL AND quantity = 5;

SELECT *
FROM cafe_staging2
WHERE quantity = 5;

-- updating the total spent column

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL OR total_spent = '';

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL AND price_per_unit = 1;

UPDATE cafe_staging2
SET total_spent = 1.0
WHERE total_spent IS NULL AND price_per_unit = 1;

SELECT *
FROM cafe_staging2
WHERE total_spent = 1.0 AND price_per_unit = 1;

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL AND price_per_unit = 2;

UPDATE cafe_staging2
SET total_spent = 2.0
WHERE total_spent IS NULL AND price_per_unit = 2;

SELECT *
FROM cafe_staging2
WHERE total_spent = 2.0 AND price_per_unit = 2;

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL AND price_per_unit = 3;

UPDATE cafe_staging2
SET total_spent = 3.0
WHERE total_spent IS NULL AND price_per_unit = 3;

SELECT *
FROM cafe_staging2
WHERE total_spent = 3.0 AND price_per_unit = 3;

SELECT *
FROM cafe_staging2
WHERE total_spent = 'UNKNOWN' OR 'ERROR';

UPDATE cafe_staging2
SET total_spent = '30.0'
WHERE total_spent = 'Marmalade cake';

UPDATE cafe_staging2
SET total_spent = '35.5'
WHERE total_spent = 'ERROR';

SELECT *
FROM cafe_staging2
WHERE item = 'UNKNOWN';

UPDATE cafe_staging2
SET item = 'Fried chicken and sauce'
WHERE item = 'UNKNOWN';

UPDATE cafe_staging2
SET item = 'Marmalade cake'
WHERE item = 'ERROR';

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL AND price_per_unit = 4;

UPDATE cafe_staging2
SET total_spent = '23.5'
WHERE total_spent IS NULL AND price_per_unit = 4;

UPDATE cafe_staging2
SET total_spent = '36.6'
WHERE total_spent = '18.2' AND price_per_unit = 5;

SELECT *
FROM cafe_staging2
WHERE total_spent IS NULL;

UPDATE cafe_staging2
SET total_spent = '18.2'
WHERE total_spent IS NULL AND price_per_unit = 1.5;

SELECT *
FROM cafe_staging2;

SELECT *
FROM cafe_staging2
WHERE payment_method IS NULL;

SELECT *
FROM cafe_staging2
WHERE payment_method = 'Credit Card';

UPDATE cafe_staging2 
SET payment_method = NULL
WHERE payment_method = 'NULL' ; 

-- mistake
UPDATE cafe_staging2 
SET payment_method = 'NULL'
WHERE payment_method = 'UNKNOWN' ; 
-- Mistake
UPDATE cafe_staging2 
SET payment_method = 'NULL'
WHERE payment_method = 'ERROR';

SELECT *
FROM cafe_staging2
WHERE payment_method = 'UNKNOWN' AND payment_method = 'ERROR';

UPDATE cafe_staging2
SET payment_method = 'Cash'
WHERE transaction_date BETWEEN '2023-01-01' AND '2023-01-31';

UPDATE cafe_staging2
SET payment_method = 'Cash'
WHERE transaction_date BETWEEN '2023-02-01' AND '2023-02-28';

SELECT *
FROM cafe_staging2;

UPDATE cafe_staging2
SET payment_method = 'Digital Wallet'
WHERE transaction_date BETWEEN '2023-03-01' AND '2023-03-31';

UPDATE cafe_staging2
SET payment_method = 'Cash'
WHERE transaction_date BETWEEN '2023-04-01' AND '2023-04-22';

UPDATE cafe_staging2
SET payment_method = 'Digital Wallet'
WHERE transaction_date BETWEEN '2023-04-23' AND '2023-04-31';

SELECT *
FROM cafe_staging2
WHERE transaction_date BETWEEN '2023-05-01' AND '2023-05-31';

UPDATE cafe_staging2
SET payment_method = 'Digital Wallet'
WHERE transaction_date BETWEEN '2023-05-01' AND '2023-05-10';

UPDATE cafe_staging2
SET payment_method = 'Cash'
WHERE transaction_date BETWEEN '2023-05-11' AND '2023-05-20';

UPDATE cafe_staging2
SET payment_method = 'Credit Card'
WHERE transaction_date BETWEEN '2023-05-21' AND '2023-05-31';

UPDATE cafe_staging2
SET payment_method = 'Credit Card'
WHERE payment_method IS NULL;

SELECT *
FROM cafe_staging2
WHERE transaction_date BETWEEN '2023-06-01' AND '2023-06-31';

UPDATE cafe_staging2
SET location = null
WHERE location = 'UNKNOWN';

UPDATE cafe_staging2
SET location = null
WHERE location = 'ERROR';

UPDATE cafe_staging2 c1
JOIN cafe_staging2 C2
   ON c1.transaction_date = c2.transaction_date
SET c1.location = c2.location
WHERE (c1.location IS NULL) AND c2.location IS NOT NULL;

SELECT *
FROM cafe_staging2;