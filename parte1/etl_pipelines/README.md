# Entregable 2 - Materia: Análisis Exploratorio y Curación de Datos

Debe instalar Python Versión 3.6 o Superior.

Para reproducir los experimentos, deberá instalar el entorno de Anaconda.

## Variables de Entorno

Para trabajar con SQLite, se trabajo con la librería de SQLAlchemy y python-decouple:

  1. Se crea un archivo .env, en el mismo directorio que el script "main.py" donde se almacena las características de la base de datos donde vamos a trabajar.
  2. Las variables son leídas en el script main.py, a través de la importación del método config de la librería decouple.

## Ejecutar Pipeline

Para ejecutar el pipeline debe acceder a una consola y dirigirse al directorio donde se encuentra el script main.py; una vez dentro del directorio, ejecutar "python main.py"