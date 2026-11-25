#!/bin/bash
# Runs inside the one-shot `mssql-init` container.
#  1. wait until SQL Server accepts connections
#  2. load Chinook_SqlServer.sql if the Chinook database does not exist yet
#  3. enable CDC on the database and tables
set -euo pipefail

SQLCMD=(/opt/mssql-tools18/bin/sqlcmd -C -S "${MSSQL_HOST}" -U sa -P "${MSSQL_SA_PASSWORD}" -b)

for i in $(seq 1 60); do
  if "${SQLCMD[@]}" -Q "SELECT 1" >/dev/null 2>&1; then break; fi
  echo "waiting for SQL Server ($i/60)..."; sleep 3
done

EXISTS=$("${SQLCMD[@]}" -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name = 'Chinook'" | tr -d '[:space:]')

if [ "$EXISTS" = "0" ]; then
  if [ ! -f /sql/Chinook_SqlServer.sql ]; then
    echo "ERROR: /sql/Chinook_SqlServer.sql not found. Download it from"
    echo "       https://github.com/lerocha/chinook-database/releases and put it in sql/mssql/"
    exit 1
  fi
  echo "Loading Chinook database..."
  "${SQLCMD[@]}" -f 65001 -i /sql/Chinook_SqlServer.sql
else
  echo "Chinook database already exists - skipping load."
fi

echo "Enabling CDC..."
"${SQLCMD[@]}" -i /sql/enable_cdc.sql

# give the SQL Agent capture job a moment, then show capture instances
sleep 5
"${SQLCMD[@]}" -d Chinook -Q "SELECT capture_instance FROM cdc.change_tables ORDER BY 1"
echo "mssql-init finished."
