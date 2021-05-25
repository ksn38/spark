drop table if exists orders;
create table orders (
household_key int, 
basket_id bigint, 
day int, 
product_id int, 
quantity int, 
sales_value float, 
store_id int, 
retail_disc float, 
trans_time int, 
week_no int, 
coupon_disc float, 
coupon_match_disc float
);

\copy orders FROM '/home/ksn38/data/transaction_data.csv' DELIMITER ',' CSV HEADER


drop function if exists orders();
create function orders() returns void as $$
declare
begin
    copy (select household_key, product_id, quantity, week_no from orders) to '/tmp/orders.csv' with csv header;
end;
$$ language plpgsql;


