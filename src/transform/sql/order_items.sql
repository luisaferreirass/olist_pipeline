SELECT
    NULLIF(TRIM(order_id), '') AS order_id,
    CAST(NULLIF(TRIM(order_item_id), '') AS INTEGER) AS order_item_id,
    NULLIF(TRIM(product_id), '') AS product_id,
    NULLIF(TRIM(seller_id), '') AS seller_id,
    TRY_CAST(NULLIF(TRIM(shipping_limit_date), '') AS TIMESTAMP ) AS shipping_limit_date,
    CAST(NULLIF(TRIM(price), '') AS DECIMAL(8, 2)) AS price,
    CAST(NULLIF(TRIM(freight_value), '') AS DECIMAL(6, 2)) AS freight_value
FROM bronze