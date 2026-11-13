-- Só as regras gerais. Categoria sem tradução vira 'no_translation' na dim_product (Gold).
SELECT
    NULLIF(TRIM(product_category_name), '') as product_category_name,
    NULLIF(TRIM(product_category_name_english), '') as product_category_name_english
FROM bronze
