//dds слой

CREATE TABLE IF NOT EXISTS brand (
    brand_id INT PRIMARY KEY,
    brand VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS category (
    category_id VARCHAR PRIMARY KEY,
    category_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS product (
    product_id INT PRIMARY KEY,
    name_short VARCHAR,
    category_id VARCHAR(255),
    pricing_line_id INT,
    brand_id INT,
    FOREIGN KEY (category_id) REFERENCES category(category_id),
    FOREIGN KEY (brand_id) REFERENCES brand(brand_id)
);

CREATE TABLE IF NOT EXISTS stock (
    available_on INT,
    product_id INT,
    pos VARCHAR(255),
    available_quantity FLOAT,
    cost_per_item FLOAT,
    PRIMARY KEY (product_id, pos, available_quantity),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);
CREATE TABLE transaction (
    transaction_id varchar,
    product_id int,
    recorded_on date,
    quantity float,
    price float,
    price_full float,
    order_type_id varchar,
    PRIMARY KEY (transaction_id, product_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);

CREATE TABLE stores (
    transaction_id varchar,
    pos varchar)

//data mart cлой

CREATE TABLE sales (
  transaction_id varchar,
  product_id int,
  name_short varchar,
  quantity float,
  price float,
  price_full float,
  cost_per_item float,
  value_by_money float,
  brand varchar,
  category_name varchar,
  recorded_on date,
  pos varchar)


CREATE TABLE IF NOT EXISTS stock (
    product_id INT,
    name_short varchar,
    available_quantity FLOAT,
    pos VARCHAR(255),
    brand varchar,
    category_name varchar,
    cost_per_item FLOAT,
    little_reserve bool);


// exeptions layer 



CREATE TABLE error_brand (
    brand_id VARCHAR ,
    brand VARCHAR
);

CREATE TABLE IF NOT EXISTS error_category (
    category_id VARCHAR,
    category_name VARCHAR
);

CREATE TABLE IF NOT EXISTS error_product (
    product_id VARCHAR,
    name_short VARCHAR,
    category_id VARCHAR,
    pricing_line_id VARCHAR,
    brand_id VARCHAR,
    error VARCHAR
);

CREATE TABLE IF NOT EXISTS error_stock (
    available_on VARCHAR,
    product_id VARCHAR,
    pos VARCHAR,
    available_quantity VARCHAR,
    cost_per_item VARCHAR,
    error VARCHAR
);
CREATE TABLE error_transaction (
    transaction_id VARCHAR,
    product_id VARCHAR,
    recorded_on VARCHAR,
    quantity VARCHAR,
    price VARCHAR,
    price_full VARCHAR,
    order_type_id VARCHAR,
    error VARCHAR
);

CREATE TABLE pos (
    transaction_id varchar,
    pos varchar, 
    error VARCHAR)

