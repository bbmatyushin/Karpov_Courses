#!/bin/bash

echo -e "\n /* ============== step 0 ==============  */ \n\t    Создание базы данных\n" && \
hive -f `pwd`/0_create_db.sql && \

echo -e "\n /* ============== step 1 ==============  */ \nСоздание и наполнение таблиц-справочников\n" && \
hive -f `pwd`/1_create_tbl_discr.sql && \

echo -e "\n /* ============== step 2 ==============  */ \n\
 Создать таблицу фактов поездок желтого такси\n" && \
hive -f `pwd`/2_create_fact_tbl.sql

echo -e "\n /* ============== step 3 ==============  */ \n\
 Наполнение таблицы-фактов данными с бакета на yandexcloud\n" && \
hive -f `pwd`/3_load_fact_tbl.sql

echo -e "\n /* ============== step 4 ==============  */ \n\
 Создать представление (view) для вычисления витрины\n" && \
hive -f `pwd`/4_create_view.sql

echo -e "\n /* ============== step 5 ==============  */ \n\
   Создать таблицу — витрину вида:\n" && \
hive -f `pwd`/5_create_showcase_tbl.sql

echo -e "\n /* ============== step 6 ==============  */ \n\
\tНаполнение таблицы-витрина\n" && \
hive -f `pwd`/6_insert_showcase_tbl.sql



