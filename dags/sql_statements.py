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
        origem_endereco	 VARCHAR(500), 
        origem_bairro  VARCHAR(500), 
        origem_cidade  VARCHAR(500), 
        origem_uf  VARCHAR(500), 
        destino_solicitado_endereco  VARCHAR(500), 
        destino_efetivo_endereco  VARCHAR(500), 
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