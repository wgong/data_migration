https://stackoverflow.com/questions/9736085/run-a-postgresql-sql-file-using-command-line-arguments
$ export PGPASSWORD=password
$ psql -h localhost -d testdb -U postgres -a -q -f ~/projects/insight_ds/data_migration/scripts/create_tables.sql
