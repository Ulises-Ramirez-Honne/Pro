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
import io

# definir las porpiedades que se usaran 
s3 = boto3.client('s3')
bedrock = boto3.client('bedrock-runtime')
DESTINATION_BUCKET = "silver-honne-sep"

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

def clasificacion_documentos(bucket, key):
    file_extension = key.split('.')[-1].lower()
    response = s3.get_object(Bucket=bucket, Key=key)
    file_bytes = response['Body'].read()
    inf_params = {"maxTokens": 5000, "temperature": 0.1}

    messages = [{
        "role": "user",
        "content": [
            {
                "document": {
                    "format": file_extension,
                    "name": f"Documento_{file_extension}",
                    "source": {"bytes": file_bytes}
                }
            },
            {
            "text": "Eres un experto en clasificar documentos oficiales mexicanos.\n" + \
                        "Clasifícalo, de acuerdo a su contenido y características visuales, en alguno de los siguientes tipos de documento:\n" + \
                        "- \"INE_IFE\"; Credencial para votar de mexico\n" + \
                        "- \"Poder_Notarial\"; Contiene un encabezado de identificacion del notario ,ademas poliza del libro notarial y Firma del Notario/Corredor Público y Sello.\n" + \
                        "- \"Acta_Constitutiva\"; Aunque el documento en sí es una Escritura Pública, Instrumento Notarial o Póliza, su contenido se referiere explícitamente a la Constitución de la Sociedad Mercantil o la formalización de actos relacionados con la vida de una sociedad, es indispensable la participación de un Notario Público o Corredor Público, quien HACE CONSTAR  el acto, los datos del Notario Público o Corredor Público (nombre, número, lugar de adscripción) y sellos son prominentes, se identifican claramente las personas físicas o morales que constituyen la sociedad.\n" + \
                        "- \"Constancia_de_situación_fiscal\";Documento oficial emitido por el SAT que incluye el logotipo del SAT, datos de identificación del contribuyente (nombre, RFC, CURP), domicilio fiscal, datos de ubicación, actividades económicas, regímenes, obligaciones, y fecha de inicio de operaciones. Suele tener un código QR y una leyenda que indica su validez oficial.\n" + \
                        "- \"Declaración_Anual_del_SAT\"; Contiene frases explicitas como DECLARACIÓN DEL EJERCICIO DE IMPUESTOS FEDERALES, tiene presencia de los logotipos o menciones de HACIENDA y SAT (Servicio de Administración Tributaria), cuenta con la Indicación clara del año fiscal al que corresponde la declaración, por ejemplo, Ejercicio: 2023 .\n" + \
                        "- \"Opiniones_de_cumplimiento\" (SAT, IMSS, INFONAVIT).\n" + \
                        "- \"Opinión del cumplimiento de obligaciones fiscales en materia de Seguridad Social\"; Este archivo lo crea la institución llamada Instituto Mexicano del Seguro Social (IMSS).\n" + \
                        "- \"Opinión del cumplimiento de obligaciones fiscales\"; Servicio de Administración Tributaria. Este archivo lo crea la institución llamada SAT.\n" + \
                        "- \"Opinión del cumplimiento de INFONAVIT\"; Usualmente el documento tiene como título: Constancia de Situación Fiscal en materia obligaciones Fiscales relativa a las aportaciones patronales y entero de descuentos. Este archivo lo crea la institución llamada INFONAVIT.\n" + \
                        "- \"Documento desconocido\"; Cuando no se tiene la certeza del tipo de documento o no entra en los tipos anteriomente descritos.\n" + \
                        "- \"Estado_de_cuenta\"; Documento bancario que muestra los movimientos y saldo de una cuenta. Generalmente incluye el nombre del banco, número de cuenta, nombre del titular, periodo del estado de cuenta, lista de transacciones con fechas y montos, y saldo inicial y final.\n" + \
                        "- \"Comprobante_de_domicilio\"; el cual puede ser de agua, luz, teléfono, entre otros. en ellos contienen informacion del total a pagar del servicio.\n" + \
                        "- \"Cedula_Profesional\".\n" + \
                        "- \"Curriculum_vitae\"; Este archivo a veces tiene la palabra Curriculum. Este archivo contiene el perfil de una empresa. Usualmente contiene información sobre la historia, la misión, los servicios que ofrecen y los proyectos realizados por la empresa. Usualmente incluyen la información de contacto de los representantes de la empresa.\n" + \
                        "- \"Carta_Responsiva\".\n" + \
                        "Devuelve la respuesta en formato JSON, con la clave principal \"tipoDocumento\", solamentamente el tipo documento (nada de descripcion que van despues del punto y coma).\n" + \
                        "Por favor y muchas gracias."
            }
        ]
    }]
    response = bedrock.converse(
        modelId="us.amazon.nova-lite-v1:0",
        messages=messages,
        inferenceConfig=inf_params
    )

    raw_text = response['output']['message']['content'][0]['text']
    return raw_text

def upload_roi_to_s3(roi_np, filename):
    """Convierte el ROI a imagen PNG y lo sube a S3."""
    roi_image = Image.fromarray(roi_np)
    buffer = io.BytesIO()
    roi_image.save(buffer, format="PNG")
    buffer.seek(0)
    s3.upload_fileobj(buffer, DESTINATION_BUCKET, filename)
    print(f"[{execution_id}] Imágen {filename} cargada a {DESTINATION_BUCKET}")

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
        filename = f"pagina_{page_num}_area_{i+1}.png"
        upload_roi_to_s3(roi, filename)
         
 
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
    # se calsificara el documento 
    tipo = clasificacion_documentos(bucket, key)
    print(f"Tipo: {tipo}")
    extract_from_pdf_pymupdf(local_path)

end_time = time.time()
end_dt = datetime.utcnow()
duration = round(end_time - start_time, 2)

print(f"[{execution_id}] Contenedor finalizado a las {end_dt} UTC")
print(f"[{execution_id}] Duración total: {duration} segundos")
print(f"----------------------------------------------------------------")
