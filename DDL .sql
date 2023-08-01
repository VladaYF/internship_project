

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
    PRIMARY KEY (available_on, product_id, pos, available_quantity),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);
CREATE TABLE transaction (
    transaction_id varchar,
    product_id int,
    recorded_on timestamp,
    quantity float,
    price float,
    price_full float,
    order_type_id varchar,
    PRIMARY KEY (transaction_id, product_id),
    FOREIGN KEY (product_id) REFERENCES product(product_id)
);