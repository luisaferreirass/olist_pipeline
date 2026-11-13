-- dim_date: 1 linha por dia, de 2016-01-01 a 2019-12-31 (dados vão de set/2016 a out/2018; folga dos dois lados).
-- Gerada, não vem da fonte. Intervalo fixo: recriar todo dia dá sempre o mesmo resultado.
-- Chave: date_key no formato AAAAMMDD (estável e legível; exceção à chave sequencial das outras dimensões).
WITH dias AS (
    SELECT explode(sequence(DATE '2016-01-01', DATE '2019-12-31', INTERVAL 1 DAY)) AS date
)
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    date,
    year(date)                                 AS year,
    quarter(date)                              AS quarter,
    month(date)                                AS month,
    CASE month(date)
        WHEN 1 THEN 'janeiro'   WHEN 2 THEN 'fevereiro' WHEN 3 THEN 'março'
        WHEN 4 THEN 'abril'     WHEN 5 THEN 'maio'      WHEN 6 THEN 'junho'
        WHEN 7 THEN 'julho'     WHEN 8 THEN 'agosto'    WHEN 9 THEN 'setembro'
        WHEN 10 THEN 'outubro'  WHEN 11 THEN 'novembro' WHEN 12 THEN 'dezembro'
    END                                        AS month_name,
    day(date)                                  AS day,
    weekday(date) + 1                          AS day_of_week,  -- 1 = segunda ... 7 = domingo (ISO)
    CASE weekday(date) + 1
        WHEN 1 THEN 'segunda' WHEN 2 THEN 'terça'  WHEN 3 THEN 'quarta' WHEN 4 THEN 'quinta'
        WHEN 5 THEN 'sexta'   WHEN 6 THEN 'sábado' WHEN 7 THEN 'domingo'
    END                                        AS day_name,
    weekday(date) + 1 IN (6, 7)                AS is_weekend
FROM dias
