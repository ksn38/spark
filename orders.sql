-- psql -U ksn38 -d sparkdb -a -f orders.sql && python3 fit_als.py && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 trained_als_with_stream.py

--\copy (select * from mybl_lang) to '/tmp/orders.csv' with csv
select orders();
