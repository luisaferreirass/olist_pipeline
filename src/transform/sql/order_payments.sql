-- not_defined e parcelas = 0 não têm significado → NULL (tratamento_silver.md).
-- payment_sequential fica como veio (sem renumerar): faz parte da chave do upsert.
WITH tipado AS (
    SELECT
        NULLIF(TRIM(order_id), '') as order_id,
        CAST(NULLIF(TRIM(payment_sequential), '') AS INTEGER) as payment_sequential,
        NULLIF(NULLIF(TRIM(payment_type), ''), 'not_defined') as payment_type,
        NULLIF(CAST(NULLIF(TRIM(payment_installments), '') AS INTEGER), 0) as payment_installments,
        CAST(NULLIF(TRIM(payment_value), '') AS DECIMAL(8, 2)) as payment_value
    FROM bronze
)
SELECT
    *,
    payment_value = 0 as flag_valor_zero
FROM tipado
