-- Chave = (review_id, order_id): o mesmo review_id aparece em pedidos diferentes (tratamento_silver.md).
-- Quebras de linha nos comentários ficam como vieram; só as das pontas saem no TRIM,
-- senão mensagem que é só quebra de linha não vira NULL (o TRIM padrão só tira espaço).
SELECT
    NULLIF(TRIM(review_id), '') as review_id,
    NULLIF(TRIM(order_id), '') as order_id,
    CAST(NULLIF(TRIM(review_score), '') AS INTEGER) as review_score,
    NULLIF(TRIM(review_comment_title), '') as review_comment_title,
    NULLIF(TRIM(review_comment_message, ' ' || chr(13) || chr(10)), '') as review_comment_message, -- chr(13) = \r e chr(10) = \n
    TRY_CAST(NULLIF(TRIM(review_creation_date), '') AS TIMESTAMP) as review_creation_date,
    TRY_CAST(NULLIF(TRIM(review_answer_timestamp), '') AS TIMESTAMP) as review_answer_timestamp
FROM bronze
