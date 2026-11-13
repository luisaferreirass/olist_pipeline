-- Limpeza da cidade adiada (tratamento_silver.md): só as regras gerais.
-- Exceção: os 2 valores que não são cidade viram NULL; o vendedor fica.
SELECT
    NULLIF(TRIM(seller_id), '')              AS seller_id,
    NULLIF(TRIM(seller_zip_code_prefix), '') AS seller_zip_code_prefix,
    CASE
        WHEN TRIM(seller_city) IN ('04482255', 'vendas@creditparts.com.br') THEN NULL
        ELSE NULLIF(TRIM(seller_city), '')
    END                                      AS seller_city,
    NULLIF(TRIM(seller_state), '')           AS seller_state
FROM bronze
