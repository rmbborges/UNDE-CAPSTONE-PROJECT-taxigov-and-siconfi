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

CREATE_RAW__PUBLIC_EXPENSES_DATA = """
    DROP TABLE IF EXISTS raw__public_expenses_data;

    CREATE TABLE IF NOT EXISTS raw__public_expenses_data (
        co_siorg_n04 VARCHAR(20),
        ds_siorg_n04 VARCHAR(500),
        co_siorg_n05 VARCHAR(20),
        ds_siorg_n05 VARCHAR(500),
        co_siorg_n06 VARCHAR(20),
        ds_siorg_n06 VARCHAR(500),
        co_siorg_n07 VARCHAR(20),
        ds_siorg_n07 VARCHAR(500),
        me_referencia INTEGER NOT NULL,
        an_referencia INTEGER NOT NULL,
        sg_mes_completo VARCHAR(20),
        me_emissao INTEGER,
        an_emissao INTEGER,
        co_situacao_icc VARCHAR(20),
        no_situacao_icc VARCHAR(500),
        id_natureza_juridica_siorg VARCHAR(20),
        ds_natureza_juridica_siorg VARCHAR(200),
        id_categoria_economica_nade VARCHAR(20),
        id_grupo_despesa_nade VARCHAR(20),
        id_moap_nade VARCHAR(20),
        id_elemento_despesa_nade VARCHAR(500),
        id_subitem_nade VARCHAR(500),
        co_natureza_despesa_deta VARCHAR(500),
        no_natureza_despesa_deta VARCHAR(500),
        id_esfera_orcamentaria VARCHAR(500),
        no_esfera_orcamentaria VARCHAR(500),
        id_in_resultado_eof VARCHAR(500),
        no_in_resultado_eof VARCHAR(500),
        va_custo DECIMAL(16,2)
    )
    DISTSTYLE AUTO;
"""

CREATE_DIM_COST_CENTERS = """
    DROP TABLE IF EXISTS dim_cost_centers;

    CREATE TABLE dim_cost_centers AS (
        SELECT
            DISTINCT
                COALESCE(
                    IF(co_siorg_n07 != "-9", co_siorg_n07, NULL), 
                    IF(co_siorg_n06 != "-9", co_siorg_n06, NULL), 
                    IF(co_siorg_n05 != "-9", co_siorg_n05, NULL), 
                    IF(co_siorg_n04 != "-9", co_siorg_n04, NULL)
                ) AS id,
                COALESCE(
                    IF(co_siorg_n07 != "-9", ds_siorg_n07, NULL), 
                    IF(co_siorg_n06 != "-9", ds_siorg_n06, NULL), 
                    IF(co_siorg_n05 != "-9", ds_siorg_n05, NULL), 
                    IF(co_siorg_n04 != "-9", ds_siorg_n04, NULL)
                ) AS name
        FROM
            raw__public_expenses_data
    );
"""

CREATE_DIM_COST_CENTERS_RELATIONSHIP = """
    DROP TABLE IF EXISTS dim_cost_centers_relationship;

    CREATE TABLE dim_cost_centers_relationship AS (
        SELECT
            DISTINCT
                co_siorg_n04 AS first_level_cost_center,
                COALESCE(
                    IF(co_siorg_n05 != "-9", co_siorg_n05, NULL),
                    co_siorg_n04
                ) AS second_level_cost_center,
                COALESCE(
                    IF(co_siorg_n06 != "-9", co_siorg_n06, NULL),
                    IF(co_siorg_n05 != "-9", co_siorg_n05, NULL),
                    co_siorg_n04
                ) AS third_level_cost_center,
                COALESCE(
                    IF(co_siorg_n07 != "-9", co_siorg_n07, NULL),
                    IF(co_siorg_n06 != "-9", co_siorg_n06, NULL),
                    IF(co_siorg_n05 != "-9", co_siorg_n05, NULL),
                    co_siorg_n04
                ) AS fourth_level_cost_center,
        FROM
            raw__public_expenses_data
    );
"""

CREATE_DIM_EXPENSES = """
    DROP TABLE IF EXISTS dim_expenses;

    CREATE TABLE dim_expenses AS (
        SELECT
            SHA2(
                CONCAT(
                    co_situacao_icc, 
                    co_natureza_despesa_deta,
                    CAST(me_referencia AS VARCHAR),
                    CAST(an_referencia AS VARCHAR),
                    CAST(co_natureza_despesa_deta AS VARCHAR)
                ),
                256
            ) AS id,
            an_reference AS reference_year,
            me_referencia AS reference_month,
            COALESCE(
                IF(co_siorg_n07 != "-9", co_siorg_n07, NULL), 
                IF(co_siorg_n06 != "-9", co_siorg_n06, NULL), 
                IF(co_siorg_n05 != "-9", co_siorg_n05, NULL), 
                IF(co_siorg_n04 != "-9", co_siorg_n04, NULL)
            ) AS cost_center,
            no_esfera_orcamentaria AS expense_category,
            no_natureza_despesa_deta AS expense_detail,
            va_custo AS expense_value
        FROM
            raw__public_expenses_data
    );
"""

CREATE_FACT_MONTHLY_EXPENSES = """
    DROP TABLE IF EXISTS fact_monthly_expenses;

    CREATE TABLE fact_monthly_expenses AS (
        SELECT
            reference_year,
            reference_month,
            first_level_cost_center,
            second_level_cost_center,
            third_level_cost_center,
            fourth_level_cost_center,
            SUM(cost) AS total_expense,
            AVG(cost) AS average_expense
        FROM
            dim_expenses
        LEFT JOIN
            dim_cost_centers_relationship ON dim_expenses.cost_center = dim_cost_centers_relationship.fourth_level_cost_center
        GROUP BY 
            reference_year,
            reference_month,
            first_level_cost_center,
            second_level_cost_center,
            third_level_cost_center,
            fourth_level_cost_center
    );
"""