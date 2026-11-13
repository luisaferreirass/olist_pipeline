-- Silver mantém 1 linha por ponto; a consolidação por CEP (mediana) fica na Gold (tratamento_silver.md).
-- Fora do Brasil = fora do retângulo do país. O limite leste vai até -28.8 (e não -34.79, a ponta
-- do continente) para incluir as ilhas oceânicas, como Fernando de Noronha (lng ≈ -32.4).
WITH tipado AS (
    SELECT
        NULLIF(TRIM(geolocation_zip_code_prefix), '') as geolocation_zip_code_prefix,
        TRY_CAST(NULLIF(TRIM(geolocation_lat), '') AS DOUBLE) as geolocation_lat,
        TRY_CAST(NULLIF(TRIM(geolocation_lng), '') AS DOUBLE) as geolocation_lng,
        NULLIF(TRIM(geolocation_city), '') as geolocation_city,
        NULLIF(TRIM(geolocation_state), '') as geolocation_state
    FROM bronze
)
SELECT DISTINCT *
FROM tipado
WHERE geolocation_lat BETWEEN -33.75 AND 5.27
  AND geolocation_lng BETWEEN -73.99 AND -28.8
