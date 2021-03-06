# Рекомендательная система на основе Spark ML  
Используется модель коллаборативной фильтрации Alternating Least Squares (ALS). В ее основе используется разложение изначальной матрицы R с двумя размерностями: пользователи и товары. Изначальная матрица R получается большой и разряженной. ALS раскладывает ее на 2 матрицы меньшей размерности X * Yt = R. При этом на каждой итерации первая из двух меньших матриц становится постоянной и подгоняется вторая матрица. На следующем шаге уже вторая матрица становится постоянной, а подгоняется первая с помощью метода наименьших квадратов.  
Данные для обучения выгружаются из PosgreSQL с помощью вызова функции orders().  
На них обучается ALS и извлекаются 5 наиболее популярных товаров, которые заменяют рекомендуемые товары в случае, если пользователь ничего не покупал или заходит без авторизации.  
ID пользователя передается через кафку в формате JSON. Время хранения сообщения сокращено до 10 секунд.  
В конечном итоге все собирается в Spark Streaming и выводится в консоль.  
Команда для запуска системы: psql -U ksn38 -d sparkdb -a -f orders.sql && python3 fit_als.py && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 trained_als_with_stream.py  

Блок-схема:
![spark-als](https://github.com/ksn38/spark/blob/main/spark-als.png)
