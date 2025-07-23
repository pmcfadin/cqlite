CREATE TABLE ecommerce.orders (
    order_id uuid,
    customer_id uuid,
    created_at timestamp,
    status text,
    total_amount decimal,
    items list<frozen<map<text, text>>>,
    shipping_address frozen<map<text, text>>,
    PRIMARY KEY ((customer_id), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);