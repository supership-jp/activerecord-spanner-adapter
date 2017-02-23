CREATE TABLE products(
  id STRING(36) NOT NULL,
  name STRING(255) NOT NULL,
  description STRING(MAX),
  price INT64 NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
) PRIMARY KEY(id);

CREATE UNIQUE INDEX
  idx_products_name
ON
  products(name);

CREATE TABLE customers(
  id STRING(36) NOT NULL,
  name STRING(255) NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
) PRIMARY KEY (id);

CREATE TABLE orders(
  id STRING(36) NOT NULL,
  customer_id STRING(36) NOT NULL,
  note STRING(MAX),
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id);

CREATE INDEX idx_orders_customer_id
ON orders(customer_id)
STORING (created_at);

CREATE TABLE line_items(
  id STRING(36) NOT NULL,
  order_id STRING(36) NOT NULL,
  product_id STRING(36) NOT NULL,
  unit_price INT64 NOT NULL,
  quantity INT64 NOT NULL,
  created_at TIMESTAMP NOT NULL
) PRIMARY KEY (id),
INTERLEAVE IN PARENT orders ON DELETE CASCADE;

CREATE INDEX
  idx_line_items_order_id
ON
  line_items(order_id)
STORING (
  product_id,
  created_at
)
