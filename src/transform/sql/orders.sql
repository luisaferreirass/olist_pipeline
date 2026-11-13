-- Datas fora de ordem e status contraditório: manter como veio e sinalizar (tratamento_silver.md).
-- Comparação com NULL dá NULL; o COALESCE transforma em FALSE (etapa não aconteceu = sem violação).
WITH tipado AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        NULLIF(TRIM(customer_id), '') as customer_id,
        NULLIF(TRIM(order_status), '') as order_status,
        CAST(NULLIF(TRIM(order_purchase_timestamp), '') AS TIMESTAMP) as order_purchase_timestamp,
        TRY_CAST(NULLIF(TRIM(order_approved_at), '') AS TIMESTAMP) as order_approved_at,
        TRY_CAST(NULLIF(TRIM(order_delivered_carrier_date), '') AS TIMESTAMP) as order_delivered_carrier_date,
        TRY_CAST(NULLIF(TRIM(order_delivered_customer_date), '') AS TIMESTAMP) as order_delivered_customer_date,
        TRY_CAST(NULLIF(TRIM(order_estimated_delivery_date), '') AS TIMESTAMP) as order_estimated_delivery_date
    FROM bronze
)
SELECT
    *,
    COALESCE(order_delivered_carrier_date < order_approved_at, FALSE) as flag_carrier_antes_aprovacao, -- proteção contra o null caso uma data seja nula
    COALESCE(order_delivered_carrier_date < order_purchase_timestamp, FALSE) as flag_carrier_antes_compra,
    COALESCE(order_delivered_customer_date < order_delivered_carrier_date, FALSE) as flag_entrega_antes_carrier,
    -- etapa sem data, mas com alguma etapa seguinte preenchida
    (order_approved_at IS NULL AND (order_delivered_carrier_date IS NOT NULL OR order_delivered_customer_date IS NOT NULL))
        OR (order_delivered_carrier_date IS NULL AND order_delivered_customer_date IS NOT NULL) as flag_etapa_pulada,
    order_status = 'delivered' AND order_delivered_customer_date IS NULL as flag_delivered_sem_entrega,
    order_status = 'canceled' AND order_delivered_customer_date IS NOT NULL as flag_canceled_com_entrega
FROM tipado
