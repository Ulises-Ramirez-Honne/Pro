import boto3
import os
import sys
import json
import time
import uuid
from datetime import datetime
import numpy as np
from PIL import Image
import fitz  # PyMuPDF
import cv2
import io

# Configuraciones globales
DESTINATION_BUCKET = "silver-honne-sep"
STEP_FUNCTION_ARN = "arn:aws:states:us-east-1:153788051293:stateMachine:test_ec2"

# Inicializa clientes AWS
s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
stepfunctions = boto3.client("stepfunctions", region_name="us-east-1")

# UUID para esta ejecución
execution_id = str(uuid.uuid4())
start_time = time.time()
start_dt = datetime.utcnow()

# Leer mensaje de evento
with open(sys.argv[1], 'r') as f:
    msg = json.load(f)

bucket = msg["detail"]["bucket"]["name"]
key = msg["detail"]["object"]["key"]
event_id = msg["id"]
filename = key.split("/")[-1]
local_pdf_path = f"/tmp/{filename}"
output_pdf_path = f"/tmp/processed_{filename}"

print(f"[{execution_id}] Iniciando procesamiento para {key}")

# Descargar archivo desde S3
s3.download_file(bucket, key, local_pdf_path)
print(f"[{execution_id}] PDF descargado desde S3")

# Clasificación documental con Bedrock
def clasificacion_documentos(bucket, key):
    file_extension = key.split('.')[-1].lower()
    response = s3.get_object(Bucket=bucket, Key=key)
    file_bytes = response['Body'].read()

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

    inf_config = {"maxTokens": 5000, "temperature": 0.1}

    response = bedrock.converse(
        modelId="us.amazon.nova-lite-v1:0",
        messages=messages,
        inferenceConfig=inf_config
    )
    return response['output']['message']['content'][0]['text']

# Procesamiento de imágenes con OpenCV
def process_image_opencv(image_np):
    gray = cv2.cvtColor(image_np, cv2.COLOR_RGB2GRAY)
    return Image.fromarray(gray)

# Extraer páginas y procesar
def extract_and_process_images(pdf_path, dpi=150):
    doc = fitz.open(pdf_path)
    processed_images = []

    for i, page in enumerate(doc):
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        img_np = np.array(img)
        processed_img = process_image_opencv(img_np)
        processed_images.append(processed_img.convert("RGB"))

    doc.close()
    return processed_images

# Procesar PDF
imagenes = extract_and_process_images(local_pdf_path)

# Guardar imágenes como un solo PDF
pdf_bytes = io.BytesIO()
if imagenes:
    imagenes[0].save(pdf_bytes, format="PDF", save_all=True, append_images=imagenes[1:])
    pdf_bytes.seek(0)

    output_key = f"preprocesado/{filename}"
    s3.upload_fileobj(pdf_bytes, DESTINATION_BUCKET, output_key)
    print(f"[{execution_id}] PDF procesado y subido a: {output_key}")
else:
    print(f"[{execution_id}] No se generaron imágenes.")

# Clasificar
try:
    tipo_doc_str = clasificacion_documentos(bucket, key)
    tipo_doc = json.loads(tipo_doc_str)
except Exception as e:
    print(f"[{execution_id}] Error al clasificar: {e}")
    tipo_doc = {"tipoDocumento": "Documento desconocido"}

# Lanzar Step Function
input_sf = {
    "Bucket": DESTINATION_BUCKET,
    "Key": output_key,
    "Tipo": tipo_doc,
    "uuid": event_id
}

try:
    response = stepfunctions.start_execution(
        stateMachineArn=STEP_FUNCTION_ARN,
        name=f"exec-{execution_id}",
        input=json.dumps(input_sf)
    )
    print(f"[{execution_id}] Step Function lanzada correctamente.")
except Exception as e:
    print(f"[{execution_id}] Error al lanzar Step Function: {e}")

# Final
end_dt = datetime.utcnow()
duration = round(time.time() - start_time, 2)
print(f"[{execution_id}] Proceso terminado en {duration} segundos. Fin: {end_dt} UTC")
