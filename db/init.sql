\c postgres

DROP TABLE IF EXISTS fact_sales_order;
DROP TABLE IF EXISTS dim_counterparty;
DROP TABLE IF EXISTS dim_currency;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_design;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_payment_type;
DROP TABLE IF EXISTS dim_staff;
DROP TABLE IF EXISTS dim_transaction;
DROP TABLE IF EXISTS fact_payment;
DROP TABLE IF EXISTS fact_purchase_order;


CREATE TABLE dim_counterparty (
  counterparty_id INT PRIMARY KEY NOT NULL,
  counterparty_legal_name VARCHAR NOT NULL,
  counterparty_legal_address_line_1 VARCHAR NOT NULL,
  counterparty_legal_address_line_2 VARCHAR,
  counterparty_legal_district VARCHAR,
  counterparty_legal_city VARCHAR,
  counterparty_legal_postal_code VARCHAR NOT NULL,
  counterparty_legal_country VARCHAR NOT NULL,
  counterparty_legal_phone_number VARCHAR NOT NULL
);


CREATE TABLE dim_currency (
  currency_id INT PRIMARY KEY NOT NULL,
  currency_code VARCHAR NOT NULL,
  currency_name VARCHAR NOT NULL
);


CREATE TABLE dim_date (
  date_id DATE PRIMARY KEY NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  day_of_week INT NOT NULL,
  day_name VARCHAR,
  month_name VARCHAR,
  quarter INT NOT NULL
);


CREATE TABLE dim_design (
  design_id INT PRIMARY KEY NOT NULL,
  design_name VARCHAR NOT NULL,
  file_location VARCHAR NOT NULL,
  file_name VARCHAR NOT NULL
);


CREATE TABLE dim_location (
  location_id SERIAL PRIMARY KEY NOT NULL,
    -- DEFAULT nextval('dim_location_location_id_seq'::regclass),
  address_line_1 VARCHAR NOT NULL,
  address_linE_2 VARCHAR,
  district VARCHAR,
  city VARCHAR NOT NULL,
  postal_code VARCHAR NOT NULL,
  country VARCHAR NOT NULL,
  phone VARCHAR NOT NULL
);


CREATE TABLE dim_payment_type (
  payment_type_id INT PRIMARY KEY NOT NULL,
  payment_type_name VARCHAR NOT NULL
);


CREATE TABLE dim_staff (
  staff_id SERIAL PRIMARY KEY NOT NULL,
    -- DEFAULT nextval('dim_staff_staff_id_seq'::regclass),
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  department_name VARCHAR NOT NULL,
  location VARCHAR NOT NULL,
  email_address VARCHAR NOT NULL
);


CREATE TABLE dim_transaction (
  transaction_id INT PRIMARY KEY NOT NULL,
  transacton_type VARCHAR NOT NULL,
  sales_order_id INT,
  purchase_order_id INT
);


CREATE TABLE fact_sales_order (
  sales_record_id SERIAL PRIMARY KEY NOT NULL,
    -- DEFAULT nextval('fact_sales_order_sales_record_id_seq'::regclass),
  sales_order_id INT NOT NULL,
  created_date DATE NOT NULL,
  FOREIGN KEY (created_date) REFERENCES dim_date(date_id),
  created_time TIME NOT NULL,
  last_updated_date DATE NOT NULL,
  FOREIGN KEY (last_updated_date) REFERENCES dim_date(date_id),
  last_updated_time TIME NOT NULL,
  sales_staff_id INT NOT NULL,
  FOREIGN KEY (sales_staff_id) REFERENCES dim_staff(staff_id),
  counterparty_id INT NOT NULL,
  FOREIGN KEY (counterparty_id) REFERENCES dim_counterparty(counterparty_id),
  units_sold INT NOT NULL,
  unit_price NUMERIC(10,2) NOT NULL,
  currency_id INT NOT NULL,
  FOREIGN KEY (currency_id) REFERENCES dim_currency(currency_id),
  design_id INT NOT NULL,
  FOREIGN KEY (design_id) REFERENCES dim_design(design_id),
  agreed_payment_date DATE NOT NULL,
  FOREIGN KEY (agreed_payment_date) REFERENCES dim_date(date_id),
  agreed_delivery_date DATE NOT NULL,
  FOREIGN KEY (agreed_delivery_date) REFERENCES dim_date(date_id),
  agreed_delivery_location_id INT NOT NULL,
  FOREIGN KEY (agreed_delivery_location_id) REFERENCES dim_location(location_id)
);