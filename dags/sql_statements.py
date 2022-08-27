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

    CREATE TABLE IF NOT EXISTS dim_requests (
        id BIGINT IDENTITY NOT NULL,
        requested_by VARCHAR(500),
        requested_at TIMESTAMP,
        approved_at TIMESTAMP,
        reason VARCHAR(200),
        status VARCHAR(200),
        requested_dropoff_latitude DECIMAL(16,2),
        requested_dropoff_longitude	DECIMAL(16,2),
        commentary VARCHAR(2000)
    );
"""

CREATE_DIM_RIDES_TABLE = """
    DROP TABLE IF EXISTS dim_rides;

    CREATE TABLE IF NOT EXISTS dim_rides (
        id BIGINT IDENTITY NOT NULL,
        request_id INT,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        pickup_latitude DECIMAL(16,2),
        pickup_longitude DECIMAL(16,2),
        dropoff_latitude DECIMAL(16,2),
        dropoff_longitude DECIMAL(16,2),
        distance DECIMAL(16,2),
        cost DECIMAL(16,2) 
    );
"""

CREATE_DIM_DATES_TABLE = """
    DROP TABLE IF EXISTS dim_dates;

    CREATE TABLE IF NOT EXISTS dim_dates (
        id BIGINT IDENTITY NOT NULL,
        ts TIMESTAMP,
        date DATE,
        day INTEGER,
        month INTEGER,
        year INTEGER,
        day_of_week INTEGER,
        is_weekday BOOLEAN
    );
"""

CREATE_FACT_DAILY_RIDES_TABLE = """
    DROP TABLE IF EXISTS fact_daily_rides;

    CREATE TABLE IF NOT EXISTS fact_daily_rides (
        id BIGINT IDENTITY NOT NULL,
        date DATE,
        ride_requests_count INTEGER,
        approved_ride_requests_count INTEGER,
        total_rides_cost DECIMAL(16, 2),
        total_cost_per_kilometer DECIMAL(16, 2),
        average_time_to_approve_request DECIMAL(16, 2),
        average_ride_duration_time DECIMAL(16, 2),
        average_ride_cost DECIMAL(16, 2),
        average_ride_distance DECIMAL(16, 2)
    );
"""

