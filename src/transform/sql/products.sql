-- Categoria vazia fica NULL; o rótulo 'sem_categoria' só entra na dim_product (Gold).
-- Corrige o erro de digitação da fonte: "lenght" → "length".
-- Peso 0 é fisicamente impossível → NULL.
SELECT
    NULLIF(TRIM(product_id), '')                                   AS product_id,
    NULLIF(TRIM(product_category_name), '')                        AS product_category_name,
    CAST(NULLIF(TRIM(product_name_lenght), '') AS INTEGER)         AS product_name_length,
    CAST(NULLIF(TRIM(product_description_lenght), '') AS INTEGER)  AS product_description_length,
    CAST(NULLIF(TRIM(product_photos_qty), '') AS INTEGER)          AS product_photos_qty,
    NULLIF(CAST(NULLIF(TRIM(product_weight_g), '') AS INTEGER), 0) AS product_weight_g,
    CAST(NULLIF(TRIM(product_length_cm), '') AS INTEGER)           AS product_length_cm,
    CAST(NULLIF(TRIM(product_height_cm), '') AS INTEGER)           AS product_height_cm,
    CAST(NULLIF(TRIM(product_width_cm), '') AS INTEGER)            AS product_width_cm
FROM bronze
