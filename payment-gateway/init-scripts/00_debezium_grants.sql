-- Grants required by Debezium MySQL connector.
-- Runs BEFORE init.sql (alphabetical sort — 00_ prefix).

GRANT SELECT                         ON *.* TO 'pg_user'@'%';
GRANT RELOAD                         ON *.* TO 'pg_user'@'%';
GRANT SHOW DATABASES                 ON *.* TO 'pg_user'@'%';
GRANT REPLICATION SLAVE              ON *.* TO 'pg_user'@'%';
GRANT REPLICATION CLIENT             ON *.* TO 'pg_user'@'%';
GRANT SELECT ON performance_schema.* TO 'pg_user'@'%';

FLUSH PRIVILEGES;
