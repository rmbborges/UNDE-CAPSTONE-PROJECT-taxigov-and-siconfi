CREATE_RAW__TAXIGOV_CORRIDAS_TABLE = """
    DROP TABLE IF EXISTS raw__taxigov_corridas;

    CREATE TABLE IF NOT EXISTS raw__taxigov_corridas (
        base_origem VARCHAR(500) NOT NULL,
        qru_corrida INTEGER,	
        nome_orgao VARCHAR(500),	
        status_corrida VARCHAR(500),
        motivo_corrida VARCHAR(500),
        km_total DECIMAL(16,2),
        valor_corrida DECIMAL(16,2), 
        data_abertura TIMESTAMP,
        data_despacho TIMESTAMP,
        data_local_embarque	TIMESTAMP,
        data_inicio	TIMESTAMP,
        data_final TIMESTAMP,
        origem_endereco	VARCHAR(500), 
        origem_bairro VARCHAR(500), 
        origem_cidade VARCHAR(500), 
        origem_uf VARCHAR(500), 
        destino_solicitado_endereco VARCHAR(500), 
        destino_efetivo_endereco VARCHAR(500), 
        origem_latitude	DECIMAL(16,2),
        origem_longitude DECIMAL(16,2),
        destino_solicitado_latitude	DECIMAL(16,2),
        destino_solicitado_longitude DECIMAL(16,2),
        destino_efetivo_latitude DECIMAL(16,2),
        destino_efetivo_longitude DECIMAL(16,2),
        ateste_setorial_data TIMESTAMP,
        conteste_info VARCHAR(2000)
    )
    DISTSTYLE AUTO;
"""

CREATE_DIM_REQUESTS_TABLE = """
    DROP TABLE IF EXISTS dim_requests;

    CREATE TABLE dim_requests AS ( 
        SELECT
            SHA2(
                CONCAT(
                    CAST(data_abertura AS VARCHAR), 
                    nome_orgao
                ),
                256
            ) AS id,
            UPPER(nome_orgao) AS requested_by,
            data_abertura AS requested_at,
            data_despacho AS approved_at,
            UPPER(motivo_corrida) AS reason,
            destino_solicitado_latitude AS requested_dropoff_latitude,
            destino_solicitado_longitude AS requested_dropoff_longitude,
            conteste_info AS commentary
        FROM
            raw__taxigov_corridas
    );
"""

CREATE_DIM_RIDES_TABLE = """
    DROP TABLE IF EXISTS dim_rides;

    CREATE TABLE dim_rides AS ( 
        SELECT
            SHA2(
                qru_corrida,
                256
            ) AS id,
            SHA2(
                CONCAT(
                    CAST(data_abertura AS VARCHAR), 
                    nome_orgao
                ),
                256
            ) AS request_id,
            data_inicio AS started_at,
            data_final AS ended_at,
            origem_latitude AS pickup_latitude,
            origem_longitude AS pickup_longitude,
            destino_efetivo_latitude AS dropoff_latitude,
            destino_efetivo_longitude AS dropoff_longitude,
            km_total AS distance,
            valor_corrida AS cost
        FROM
            raw__taxigov_corridas
    );
"""

CREATE_DIM_DATES_TABLE = """
    DROP TABLE IF EXISTS dim_dates;

    CREATE TABLE dim_dates AS (
        WITH
        distinct_timestamps AS (
            SELECT 
                data_abertura AS ts
            FROM
                raw__taxigov_corridas

            UNION DISTINCT

            SELECT
                data_despacho AS ts
            FROM
                raw__taxigov_corridas 
            
            UNION DISTINCT

            SELECT
                data_local_embarque AS ts
            FROM
                raw__taxigov_corridas 

            UNION DISTINCT

            SELECT
                data_inicio AS ts
            FROM
                raw__taxigov_corridas 

            UNION DISTINCT

            SELECT
                data_final AS ts
            FROM
                raw__taxigov_corridas
        )
        
        SELECT
            DISTINCT
                ts,
                DATE(ts) AS date,
                EXTRACT(MONTH FROM ts) AS month,
                EXTRACT(YEAR FROM ts) AS year,
                EXTRACT(DOW FROM ts) AS day_of_week,
                CASE
                    WHEN EXTRACT(DOW FROM ts) IN (1, 7) THEN TRUE
                    ELSE FALSE 
                END AS is_weekend
        FROM
            distinct_timestamps
    );
"""

CREATE_FACT_DAILY_RIDES_TABLE = """
    DROP TABLE IF EXISTS fact_daily_rides;

    CREATE TABLE fact_daily_rides AS (
        WITH 
        rides AS (
            SELECT
                dim_rides.*,
                DATEDIFF(
                    MINUTE,
                    started_at,
                    ended_at
                ) AS ride_duration
            FROM
                dim_rides
        ),
        requests AS (
            SELECT
                dim_requests.*,
                DATEDIFF(
                    MINUTE,
                    requested_at,
                    approved_at
                ) AS request_sla
            FROM
                dim_requests
        )

        SELECT
            DATE(started_at) AS date,
            COUNT(DISTINCT(requests.id)) AS ride_requests_count,
            COUNT(DISTINCT(rides.id)) AS rides_count,
            SUM(cost) AS total_rides_cost,
            SUM(cost)/COUNT(DISTINCT(rides.id)) AS average_cost_per_kilometer,
            AVG(ride_duration) AS average_ride_duration,
            AVG(request_sla) AS average_ride_request_sla,
            AVG(cost) AS average_ride_cost,
            AVG(distance) AS average_ride_distance
        FROM
            rides
        LEFT JOIN 
            requests ON rides.request_id = requests.id
        GROUP BY 
            date
    );
"""
