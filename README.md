# bash
``` bash
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade build
python3 -m pip install --requirement requirements.txt 
python3 -m pip install --editable .
export DB_CONNECTION="mssql+pyodbc://[username]:[password]@[server]:[port]/[database]?driver=[driver]"
python3 scripts/run_etl.py
```

# cmd
``` cmd
python -m pip install --upgrade pip
python -m pip install --upgrade build
python -m pip install --requirement requirements.txt 
python -m pip install --editable .
setx DB_CONNECTION "mssql+pyodbc://[username]:[password]@[server]:[port]/[database]?driver=[driver]"
python scripts/run_etl.py
```

- https://hub.docker.com/_/microsoft-mssql-server
- https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver16&pivots=cs1-bash