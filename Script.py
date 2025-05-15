import sys
import json
import uuid
import time
import boto3
import os
from datetime import datetime
from botocore.exceptions import BotoCoreError, NoCredentialsError
import threading
import numpy as np
import fitz #PyMuPDF
from PIL import Image
import cv2

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
print(local_path)

# Flag para indicar si la descarga terminó
download_complete = False
download_error = None

def process_image_opencv(image_np, page_num):
    gray = cv2.cvtColor(image_np, cv2.COLOR_RGB2GRAY)
 
    # Detección de bordes y dilatación
    edges = cv2.Canny(gray, 50, 150)
    dilated = cv2.dilate(edges, np.ones((5, 5), np.uint8), iterations=1)
 
    # Encontrar contornos
    contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    contours = sorted(contours, key=cv2.contourArea, reverse=True)[:2]
 
    for i, cnt in enumerate(contours):
        x, y, w, h = cv2.boundingRect(cnt)
        roi = gray[y:y+h, x:x+w]
         
 
def extract_from_pdf_pymupdf(pdf_path, dpi=150):

    doc = fitz.open(pdf_path)
 
    for i, page in enumerate(doc):
        # Renderizar la página como imagen
        mat = fitz.Matrix(dpi / 72, dpi / 72)  # 72 es el DPI base en PDF
        pix = page.get_pixmap(matrix=mat)
 
        # Convertir a imagen numpy (RGB)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img_np = np.array(img)
 
        process_image_opencv(img_np, i + 1)
 
    doc.close()

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
    extract_from_pdf_pymupdf(local_path)

end_time = time.time()
end_dt = datetime.utcnow()
duration = round(end_time - start_time, 2)

print(f"[{execution_id}] Finalizado a las {end_dt} UTC")
print(f"[{execution_id}] Duración total: {duration} segundos")
print(f"----------------------------------------------------------------")
