source .env
export PGPASSWORD=$DATABASE_PASSWORD
psql --host=$DATABASE_IP --port=$DATABASE_PORT --username=$DATABASE_USERNAME --dbname=$DATABASE_NAME -f deloton_schema.sql