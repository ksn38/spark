drop function if exists orders();
create function orders() returns void as $$
declare
begin
    copy (select * from mybl_lang) to '/tmp/orders.csv' with csv;
end;
$$ language plpgsql;


copy (select * from mybl_lang) to '/tmp/orders.csv' with csv delimiter ',' header;

\copy (select * from mybl_lang) to '/tmp/orders.csv' with csv

psql -U ksn38 -d djdb -a -f orders.sql
