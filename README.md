Спасибо за предоставленную информацию о вашем проекте! Ниже я предложу вам основные разделы, которые вы можете включить в ваш README-файл для проекта на GitHub:

## Название проекта

Разработка автоматической аналитической платформы для компании ООО СтройТорг

## Описание проекта

Этот проект представляет собой разработку автоматической аналитической платформы с дашбордами для компании ООО СтройТорг. Платформа позволяет анализировать динамику продаж в разрезе каждого розничного магазина и категории товаров, а также мониторить средний чек и общую сумму выручки. Кроме того, платформа помогает отслеживать товары с низким спросом или малой маржинальностью, а также идентифицировать магазины, приносящие меньше прибыли. Вторая часть платформы предоставляет анализ складских остатков, показывает количество товаров в каждом магазине и предупреждает о товарах, которые заканчиваются и требуют срочного заказа.

## Использование

1. Запустите dag airflow с помощью ETL_dag.py, чтобы заполнить таблицы в слое dds и выполнить обработку исключений.
2. Запустите dag airflow с помощью dm_dag.py, чтобы заполнить таблицы в слое datamart.
3. Откройте файлы с расширением .twb в Tableau Desktop, чтобы просмотреть и взаимодействовать с дашбордами (необхолим ВПН). Аналог: в файле с LOD запросами указаны ссылки на дашборды для просмотра в Tableau Public (использование с ВПН)

## Архитектурное решение

В файле "Архитектурное решение.docx" описана общая структура программы и версии использованных ПО.

## Доработки

1. Исправить синхронизацию dag'ов на синхронизацию по времени.
2. Реализовать запуск функций dm_main.py и main_func.py с помощью bashoperator, переместить в папку screepts, что бы Airflow не сканировал файлы с функциями. 
3. Разнообразить отчеты в дашборде.

## Контакты

Юлдашева Влада
VladaFY - telegramm
