-- ============================================================================
-- Source OLTP schema (commerce domain). REPLICA IDENTITY FULL is required so
-- Debezium captures the full `before` image on UPDATE/DELETE.
-- ============================================================================
SET client_min_messages = warning;

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id   BIGSERIAL PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    full_name     TEXT NOT NULL,
    tier          TEXT NOT NULL DEFAULT 'standard'
                  CHECK (tier IN ('standard', 'silver', 'gold', 'platinum')),
    country       TEXT NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.products (
    product_id    BIGSERIAL PRIMARY KEY,
    sku           TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.inventory (
    inventory_id     BIGSERIAL PRIMARY KEY,
    product_id       BIGINT NOT NULL REFERENCES public.products(product_id),
    warehouse_id     TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL CHECK (quantity_on_hand >= 0),
    reorder_level    INTEGER NOT NULL DEFAULT 10,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, warehouse_id)
);

CREATE TABLE IF NOT EXISTS public.orders (
    order_id      BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT NOT NULL REFERENCES public.customers(customer_id),
    status        TEXT NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled')),
    order_total   NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (order_total >= 0),
    currency      TEXT NOT NULL DEFAULT 'USD',
    order_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.payments (
    payment_id    BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES public.orders(order_id),
    method        TEXT NOT NULL CHECK (method IN ('card','paypal','apple_pay','bank_transfer')),
    amount        NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    status        TEXT NOT NULL DEFAULT 'authorized'
                  CHECK (status IN ('authorized','captured','failed','refunded')),
    payment_ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full row images for reliable CDC of UPDATE/DELETE before-state.
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.products  REPLICA IDENTITY FULL;
ALTER TABLE public.inventory REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;
ALTER TABLE public.payments  REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_orders_customer ON public.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_payments_order  ON public.payments(order_id);
CREATE INDEX IF NOT EXISTS idx_inventory_prod  ON public.inventory(product_id);
