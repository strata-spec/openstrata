CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    country_code TEXT NOT NULL
);
COMMENT ON TABLE public.users IS 'Application users and account metadata.';
COMMENT ON COLUMN public.users.email IS 'Primary login email for the user.';
COMMENT ON COLUMN public.users.country_code IS 'ISO country code for user locale.';

CREATE TABLE IF NOT EXISTS public.products (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price_usd NUMERIC(12,2) NOT NULL,
    category_id BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE public.products IS 'Sellable catalog products.';
COMMENT ON COLUMN public.products.price_usd IS 'Current list price in USD.';

CREATE TABLE IF NOT EXISTS public.orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    total_usd NUMERIC(12,2) NOT NULL,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES public.users(id)
);
COMMENT ON TABLE public.orders IS 'Customer orders placed in the storefront.';
COMMENT ON COLUMN public.orders.status IS 'Order lifecycle status.';

CREATE TABLE IF NOT EXISTS public.order_items (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price_usd NUMERIC(12,2) NOT NULL,
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES public.orders(id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES public.products(id)
);
COMMENT ON TABLE public.order_items IS 'Line items within each order.';
COMMENT ON COLUMN public.order_items.quantity IS 'Number of units for the product line item.';
