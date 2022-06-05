# Entregable 2 - Materia: Análisis Exploratorio y Curación de Datos. Simple ETL Using Airflow.

Este es un ETL simple usando Airflow. Primero, obtenemos datos de las URLs provistas por la Diplo como dataframes filtrados por columnas importantes. Luego combinamos los dos dataframes utilizando "left join" y lo convertimos a CSV(transformamos). Finalmente, cargamos los datos transformados a la base de datos (load).

## Prerequisitos

### Create a VirtualEnv (for local run) in Airflow Folder

``` bash
python -m venv venv
```

### Activate the VirtualEnv (for local run)

``` bash
source venv/bin/activate
```

### Install the requirements

``` bash
pip install -r requirements.txt
```

### Install the requirements

``` bash
pip install -r requirements.txt
```

### Set Airflow Home Directory (for local run)

``` bash
export AIRFLOW_HOME=~/PATH/TO/FOLDER/airflow
```

### Initializing the Airflow DB

``` bash
airflow db init
```

### Setup Admin User

``` bash
airflow users create \
    --username username \
    --firstname firstname \
    --lastname lastnamee \
    --role Admin \
    --email email@domain.com
```

### Start Webserver

``` bash
airflow webserver --port 8080 -D
```







