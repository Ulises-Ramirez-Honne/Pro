import sys
import json
import uuid
import time
import boto3
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, NoCredentialsError
import threading

start_time = time.time()
start_dt = datetime.utcnow()
execution_id = str(uuid.uuid4())

try:
    with open(sys.argv[1], 'r') as f:
        msg = json.load(f)
except Exception as e:
    print(f"[{execution_id}] Error leyendo el archivo: {e}")
    sys.exit(1)

print(f"----------------------------------------------------------------")
print(f"[{execution_id}] Contenedor iniciado a las {start_dt} UTC")

# Obtener bucket y key del mensaje
try:
    bucket = msg['detail']['bucket']['name']
    key = msg['detail']['object']['key']
except KeyError as e:
    print(f"[{execution_id}] Error al extraer bucket/key del mensaje: {e}")
    sys.exit(1)

# Ruta local donde se guardará el archivo descargado
filename = key.split("/")[-1]
local_path = os.path.join("/tmp", filename)

# Flag para indicar si la descarga terminó
download_complete = False
download_error = None

def download_file():
    global download_complete, download_error
    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket, key, local_path)
        download_complete = True
    except NoCredentialsError:
        download_error = f"No se encontraron credenciales de AWS"
    except BotoCoreError as e:
        download_error = f"Error al descargar el archivo de S3: {e}"

# Lanzamos descarga en un hilo
thread = threading.Thread(target=download_file)
thread.start()

# Spinner simple mientras esperamos
spinner = ['|', '/', '-', '\\']
idx = 0

while not download_complete and download_error is None:
    sys.stdout.write(f"\r[{execution_id}] Descargando archivo... {spinner[idx % len(spinner)]}")
    sys.stdout.flush()
    idx += 1
    time.sleep(0.1)

# Hacemos salto de línea para que no quede el spinner pegado
print()

if download_error:
    print(f"[{execution_id}] {download_error}")
else:
    print(f"[{execution_id}] Archivo descargado correctamente en: {local_path}")

end_time = time.time()
end_dt = datetime.utcnow()
duration = round(end_time - start_time, 2)

print(f"Finalizado a las {end_dt} UTC")
print(f"[{execution_id}] Duración total: {duration} segundos")
print(f"----------------------------------------------------------------")

