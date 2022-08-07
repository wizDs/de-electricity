# bash
``` bash
python3 -m venv env
source env/bin/Activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade build
python3 -m pip install -r requirements.txt 
python3 -m pip install .
export DB_CONNECTION="{your database connection strin in sqlalchemy format}"
python3 scripts/run_etl.py
```

# cmd
``` cmd
python -m venv env
./env/Scripts/activate.bat
python -m pip install --upgrade pip
python -m pip install --upgrade build
python -m pip install -r requirements.txt 
python -m pip install .
setx DB_CONNECTION "{your database connection strin in sqlalchemy format}"
python scripts/run_etl.py
```

- https://hub.docker.com/_/microsoft-mssql-server
- https://docs.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker?view=sql-server-ver16&pivots=cs1-bash