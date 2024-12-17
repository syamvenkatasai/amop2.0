CREATE TABLE IF NOT EXISTS public.migrations_altaworx_test (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    schedule_flag BOOLEAN,
    from_database VARCHAR(255) NOT NULL,
    to_database VARCHAR(255) NOT NULL,
    table_flag BOOLEAN NOT NULL,
    from_query TEXT DEFAULT '',
    to_query TEXT DEFAULT '',
    from_table VARCHAR(255) DEFAULT '',
    to_table VARCHAR(255) DEFAULT '',
    last_migrated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_id VARCHAR(255) DEFAULT '',
    time_column_check VARCHAR(255) DEFAULT '',
    status VARCHAR(50) DEFAULT 'pending',
    migration_update JSONB DEFAULT '[]',
    log_file VARCHAR(255) DEFAULT '',
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    from_db_config JSONB NOT NULL DEFAULT '{}',
    primary_id_column VARCHAR(255) NOT NULL DEFAULT '',
    update_flag BOOLEAN,
    temp_table VARCHAR,
    table_mappings JSONB DEFAULT '[]',
    reverse_sync BOOLEAN,
    reverse_sync_mapping JSON DEFAULT '{}',
    migration_order INTEGER UNIQUE
);


CREATE TABLE IF NOT EXISTS public.migrations_bak_alltenants (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    schedule_flag BOOLEAN,
    from_database VARCHAR(255) NOT NULL,
    to_database VARCHAR(255) NOT NULL,
    table_flag BOOLEAN NOT NULL,
    from_query TEXT DEFAULT '',
    to_query TEXT DEFAULT '',
    from_table VARCHAR(255) DEFAULT '',
    to_table VARCHAR(255) DEFAULT '',
    last_migrated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_id VARCHAR(255) DEFAULT '',
    time_column_check VARCHAR(255) DEFAULT '',
    status VARCHAR(50) DEFAULT 'pending',
    migration_update JSONB DEFAULT '[]',
    log_file VARCHAR(255) DEFAULT '',
    created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    from_db_config JSONB NOT NULL DEFAULT '{}',
    primary_id_column VARCHAR(255) NOT NULL DEFAULT '',
    update_flag BOOLEAN,
    temp_table VARCHAR,
    table_mappings JSONB DEFAULT '[]',
    reverse_sync BOOLEAN,
    reverse_sync_mapping JSON DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS public.mapping_table (
    id SERIAL PRIMARY KEY,
    db_name_20 VARCHAR(100),
    transfer_name VARCHAR(100),
    db_name_10 VARCHAR(100),
    table_mapping_10_to_20 JSONB,
    col_mapping_10_to_20 JSONB DEFAULT '[]',
    db_config JSONB,
    postgres_config JSONB,
    reverse_table_mapping JSONB,
    reverse_col_mapping JSONB,
    return_params_10 JSONB,
    fk_cols_10 JSONB,
    data_from_10 JSONB,
    update_cond_cols JSONB
);

CREATE TABLE IF NOT EXISTS public.lambda_sync_jobs (
    id SERIAL PRIMARY KEY,
    lambda_name_10 VARCHAR(155),
    migration_names_list JSONB,
    key_name VARCHAR(200)
);
