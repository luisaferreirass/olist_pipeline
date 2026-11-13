-- Sem tratamento específico (tratamento_silver.md): só as regras gerais.
-- CEP fica como texto para não perder o zero à esquerda.
SELECT
    NULLIF(TRIM(customer_id), '')              AS customer_id,
    NULLIF(TRIM(customer_unique_id), '')       AS customer_unique_id,
    NULLIF(TRIM(customer_zip_code_prefix), '') AS customer_zip_code_prefix,
    NULLIF(TRIM(customer_city), '')            AS customer_city,
    NULLIF(TRIM(customer_state), '')           AS customer_state
FROM bronze
