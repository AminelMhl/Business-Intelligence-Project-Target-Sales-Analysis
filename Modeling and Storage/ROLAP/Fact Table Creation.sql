---------------------------order fact table creation-----------------------------------------
CREATE TABLE fact_order(
   order_id        VARCHAR(32) NOT NULL PRIMARY KEY
  ,customer_id     VARCHAR(32) 
  ,payment_type_id INT  
  ,product_id      VARCHAR(32) 
  ,seller_id       VARCHAR(32) 
  -------other columns-------
);

---------------------------inserting ids from the dimensions-----------------------------------------
----customer dimension----
INSERT INTO fact_order (customer_id)
SELECT customer_id FROM customer_dimension

----payment type dimension----
INSERT INTO fact_order (payment_type_id)
SELECT payment_type_id FROM payment_type_dimension

----product dimension----
INSERT INTO fact_order (product_id)
SELECT payment_type_id FROM product_dimension

----product dimension----
INSERT INTO fact_order (seller_id)
SELECT payment_type_id FROM seller_dimension