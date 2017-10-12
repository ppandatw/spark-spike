SELECT * from customer_info;

SELECT count(*) FROM customer_info WHERE metro_branch = (SELECT metro_branch FROM customer_info
WHERE store_number='643' AND customer_number='57497');

SELECT * from customer_info a INNER JOIN customer_info b on (a.metro_branch=b.metro_branch)
WHERE a.store_number='643' AND a.customer_number='57497';

SELECT *
FROM customer_info a INNER JOIN customer_info b on (a.store_number=b.store_number AND a.metro_branch=b.metro_branch)
  WHERE a.store_number='643' AND a.customer_number='57497';


WITH branch_count as (SELECT count(*) OVER (PARTITION BY metro_branch) as customer_count_in_branch from customer_info
                          WHERE store_number='643')
SELECT avg(customer_count_in_branch),min(customer_count_in_branch), max(customer_count_in_branch) from branch_count;

SELECT count(*) from customer_info WHERE metro_branch IN 
                                         (SELECT metro_branch FROM customer_info WHERE store_number='643' AND customer_number='57497');

SELECT customer_number,store_number,metro_branch FROM customer_info ORDER BY (store_number,customer_number);

WITH customer_branch_count as (SELECT count(*) OVER (PARTITION BY (customer_number,store_number) )as no_of_customers 
from customer_info)
SELECT no_of_customers from customer_branch_count WHERE no_of_customers>1;

SELECT count(*) FROM customer_info WHERE metro_branch=
                                  (SELECT metro_branch FROM customer_info WHERE store_number='643' AND customer_number='57497')
                    AND store_number='643';
  