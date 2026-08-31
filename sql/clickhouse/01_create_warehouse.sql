USE northwind_dw;


/* =========================
   DATE DIMENSION
   ========================= */

CREATE TABLE IF NOT EXISTS dim_date
(
    date_key UInt32,
    full_date_alternate_key Date,
    calendar_year UInt16,
    calendar_season UInt8,
    season_name LowCardinality(String),
    month_number_of_year UInt8,
    month_name LowCardinality(String),
    day_number_of_month UInt8,
    day_of_week UInt8,
    day_of_week_name LowCardinality(String),
    is_weekend UInt8,

    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY date_key;


/* =========================
   TYPE 1 DIMENSIONS
   ========================= */

CREATE TABLE IF NOT EXISTS dim_geography
(
    geography_key UInt64,
    country Nullable(String),
    region Nullable(String),
    city Nullable(String),
    postal_code Nullable(String),
    address Nullable(String),

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY geography_key;


CREATE TABLE IF NOT EXISTS dim_shipper
(
    shipper_key UInt64,
    shipper_alternate_key UInt32,
    company_name String,
    phone Nullable(String),

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY shipper_key;


/* =========================
   SCD TYPE 2 DIMENSIONS
   ========================= */

CREATE TABLE IF NOT EXISTS dim_customer
(
    customer_key UInt64,
    customer_alternate_key String,
    geography_key UInt64 DEFAULT 0,
    company_name String,
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),

    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    is_current UInt8 DEFAULT 1,

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY customer_key;


CREATE TABLE IF NOT EXISTS dim_employee
(
    employee_key UInt64,
    parent_employee_key UInt64 DEFAULT 0,
    employee_alternate_key UInt32,
    reports_to Nullable(UInt32),
    geography_key UInt64 DEFAULT 0,

    first_name String,
    last_name String,
    title Nullable(String),
    title_of_courtesy Nullable(String),
    birth_date Nullable(Date32),
    hire_date Nullable(Date32),
    home_phone Nullable(String),
    extension Nullable(String),
    photo_base64 String DEFAULT '',
    notes Nullable(String),
    photo_path Nullable(String),

    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    is_current UInt8 DEFAULT 1,

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY employee_key;


CREATE TABLE IF NOT EXISTS dim_supplier
(
    supplier_key UInt64,
    supplier_alternate_key UInt32,
    geography_key UInt64 DEFAULT 0,

    company_name String,
    contact_name Nullable(String),
    contact_title Nullable(String),
    phone Nullable(String),
    fax Nullable(String),
    home_page Nullable(String),

    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    is_current UInt8 DEFAULT 1,

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY supplier_key;


CREATE TABLE IF NOT EXISTS dim_product
(
    product_key UInt64,
    product_alternate_key UInt32,
    supplier_key UInt64 DEFAULT 0,

    product_name String,
    category_name Nullable(String),
    quantity_per_unit Nullable(String),
    unit_price Decimal(19, 4) DEFAULT 0,
    units_in_stock Int32 DEFAULT 0,
    units_on_order Int32 DEFAULT 0,
    reorder_level Int32 DEFAULT 0,
    discontinued UInt8 DEFAULT 0,

    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    is_current UInt8 DEFAULT 1,

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY product_key;


CREATE TABLE IF NOT EXISTS dim_territory
(
    territory_key UInt64,
    territory_alternate_key String,
    region_description Nullable(String),
    territory_description Nullable(String),

    start_date DateTime64(3, 'UTC'),
    end_date Nullable(DateTime64(3, 'UTC')),
    is_current UInt8 DEFAULT 1,

    source_hash String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY territory_key;


/* =========================
   FACT TABLES
   ========================= */

CREATE TABLE IF NOT EXISTS fact_employee_territories
(
    employee_key UInt64,
    territory_key UInt64,

    is_deleted UInt8 DEFAULT 0,
    source_lsn String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (employee_key, territory_key);


CREATE TABLE IF NOT EXISTS fact_orders
(
    order_id UInt32,

    geography_key UInt64 DEFAULT 0,
    product_key UInt64,
    customer_key UInt64 DEFAULT 0,
    employee_key UInt64 DEFAULT 0,
    shipper_key UInt64 DEFAULT 0,

    order_date_key UInt32 DEFAULT 0,
    required_date_key UInt32 DEFAULT 0,
    shipped_date_key UInt32 DEFAULT 0,

    freight Decimal(19, 4) DEFAULT 0,
    ship_name Nullable(String),

    unit_price Decimal(19, 4) DEFAULT 0,
    quantity UInt32 DEFAULT 0,
    discount Decimal(9, 6) DEFAULT 0,

    order_date Nullable(DateTime64(3, 'UTC')),
    shipped_date Nullable(DateTime64(3, 'UTC')),
    required_date Nullable(DateTime64(3, 'UTC')),

    is_deleted UInt8 DEFAULT 0,
    source_lsn String DEFAULT '',
    _batch_id UUID DEFAULT generateUUIDv4(),
    _loaded_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    _version UInt64 DEFAULT toUnixTimestamp64Milli(_loaded_at)
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (order_id, product_key);
