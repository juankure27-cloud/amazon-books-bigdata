-- Ejecutar UNA vez en la base de datos de AWS RDS, conectado con el usuario maestro.
-- Crea un usuario de SOLO LECTURA para Grafana (buena práctica de seguridad:
-- el tablero nunca debe usar la clave de administrador).

CREATE USER grafana_ro WITH PASSWORD 'CAMBIAR_POR_UNA_CLAVE_SEGURA';
GRANT CONNECT ON DATABASE amazon_books TO grafana_ro;
GRANT USAGE ON SCHEMA public TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_ro;
-- para que también pueda leer las tablas que el ETL cree en el futuro:
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO grafana_ro;

-- Verificación: tablas cargadas por el flujo de Prefect y su número de filas
SELECT relname AS tabla, n_live_tup AS filas
FROM pg_stat_user_tables ORDER BY relname;
