-- dim_seller: 1 linha por seller_id, atributos tipo 1 (sobrescritos com o estado atual da silver).
-- Chave substituta seller_sk estável: quem já existe em dim_seller_old mantém a chave;
-- os novos recebem max + 1, max + 2... em ordem de seller_id (determinístico).
-- Lat/lng = mediana dos pontos do CEP (tratamento_silver.md). LEFT JOIN: CEP sem geolocation fica com NULL.
WITH geo AS (
    SELECT
        geolocation_zip_code_prefix AS zip_code_prefix,
        percentile(geolocation_lat, 0.5) AS lat,
        percentile(geolocation_lng, 0.5) AS lng
    FROM geolocation
    GROUP BY geolocation_zip_code_prefix
),
atual AS (
    SELECT
        s.seller_id,
        s.seller_zip_code_prefix AS zip_code_prefix,
        s.seller_city            AS city,
        s.seller_state           AS state,
        g.lat,
        g.lng
    FROM sellers s
    LEFT JOIN geo g ON g.zip_code_prefix = s.seller_zip_code_prefix
),
max_seller_sk AS (
    SELECT COALESCE(MAX(seller_sk), 0) AS max_old FROM dim_seller_old
)
SELECT
    COALESCE(
        dim_old.seller_sk,
        max_seller_sk.max_old + ROW_NUMBER() OVER (PARTITION BY dim_old.seller_sk IS NULL ORDER BY a.seller_id)
    ) AS seller_sk,
    a.seller_id,
    a.zip_code_prefix,
    a.city,
    a.state,
    a.lat,
    a.lng
FROM atual a
LEFT JOIN dim_seller_old dim_old ON dim_old.seller_id = a.seller_id
CROSS JOIN max_seller_sk
