BEGIN;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(5) PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24),
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.employees (
    employee_id INTEGER PRIMARY KEY,
    last_name VARCHAR(20) NOT NULL,
    first_name VARCHAR(10) NOT NULL,
    title VARCHAR(30),
    title_of_courtesy VARCHAR(25),
    birth_date TIMESTAMP,
    hire_date TIMESTAMP,
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    home_phone VARCHAR(24),
    extension VARCHAR(4),
    photo BYTEA,
    notes TEXT,
    reports_to INTEGER,
    photo_path VARCHAR(255),
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.suppliers (
    supplier_id INTEGER PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(30),
    contact_title VARCHAR(30),
    address VARCHAR(60),
    city VARCHAR(15),
    region VARCHAR(15),
    postal_code VARCHAR(10),
    country VARCHAR(15),
    phone VARCHAR(24),
    fax VARCHAR(24),
    home_page TEXT,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.categories (
    category_id INTEGER PRIMARY KEY,
    category_name VARCHAR(15) NOT NULL,
    description TEXT,
    picture BYTEA,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(40) NOT NULL,
    supplier_id INTEGER,
    category_id INTEGER,
    quantity_per_unit VARCHAR(20),
    unit_price NUMERIC(19,4),
    units_in_stock SMALLINT,
    units_on_order SMALLINT,
    reorder_level SMALLINT,
    discontinued BOOLEAN NOT NULL,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.shippers (
    shipper_id INTEGER PRIMARY KEY,
    company_name VARCHAR(40) NOT NULL,
    phone VARCHAR(24),
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.regions (
    region_id INTEGER PRIMARY KEY,
    region_description VARCHAR(50) NOT NULL,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.territories (
    territory_id VARCHAR(20) PRIMARY KEY,
    territory_description VARCHAR(50) NOT NULL,
    region_id INTEGER NOT NULL,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.employee_territories (
    employee_id INTEGER NOT NULL,
    territory_id VARCHAR(20) NOT NULL,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (employee_id, territory_id)
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(5),
    employee_id INTEGER,
    order_date TIMESTAMP,
    required_date TIMESTAMP,
    shipped_date TIMESTAMP,
    ship_via INTEGER,
    freight NUMERIC(19,4),
    ship_name VARCHAR(40),
    ship_address VARCHAR(60),
    ship_city VARCHAR(15),
    ship_region VARCHAR(15),
    ship_postal_code VARCHAR(10),
    ship_country VARCHAR(15),
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.order_details (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    unit_price NUMERIC(19,4) NOT NULL,
    quantity SMALLINT NOT NULL,
    discount REAL NOT NULL,
    _batch_id UUID NOT NULL,
    _loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_customers_geography
    ON staging.customers (country, region, city, postal_code);

CREATE INDEX IF NOT EXISTS idx_staging_employees_reports_to
    ON staging.employees (reports_to);

CREATE INDEX IF NOT EXISTS idx_staging_products_supplier
    ON staging.products (supplier_id);

CREATE INDEX IF NOT EXISTS idx_staging_orders_order_date
    ON staging.orders (order_date);

CREATE INDEX IF NOT EXISTS idx_staging_orders_customer
    ON staging.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_staging_order_details_order
    ON staging.order_details (order_id);

COMMIT;
