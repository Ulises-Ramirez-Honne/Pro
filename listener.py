import time
import uuid
import os
import subprocess
import boto3
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# ANSI color codes para logs
class LogColor:
    RESET = '\033[0m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    CYAN = '\033[96m'

def log(msg, color=LogColor.RESET):
    print(f"{color}{msg}{LogColor.RESET}")

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/153788051293/DocumentInformationQueue'
sqs = boto3.client('sqs', region_name='us-east-1')

def timestamp():
    return time.time()

def elapsed(start):
    return f"{time.time() - start:.2f} segundos"

def handle_message(message):
    temp_id = str(uuid.uuid4())
    temp_filename = f"/tmp/message_{temp_id}.json"

    log(f"[{temp_id}] ===== INICIO =====", LogColor.CYAN)

    step_start = timestamp()
    body = message['Body']
    try:
        json.loads(body)
        with open(temp_filename, 'w') as f:
            f.write(body)
        log(f"[{temp_id}] JSON guardado en {elapsed(step_start)}", LogColor.GREEN)
    except Exception as e:
        log(f"[{temp_id}] Error al guardar JSON: {e}", LogColor.RED)
        return temp_id, "ERROR"

    step_start = timestamp()
    try:
        log(f"[{temp_id}] Ejecutando contenedor Docker...", LogColor.YELLOW)
        subprocess.run([
            'docker', 'run', '--rm',
            '-v', f'{temp_filename}:/app/message.json',
            'my-listener-image',
            '/app/message.json'
        ], check=True)
        log(f"[{temp_id}] Docker ejecutado correctamente en {elapsed(step_start)}", LogColor.GREEN)
    except subprocess.CalledProcessError as e:
        log(f"[{temp_id}] Error en ejecución de Docker: {e}", LogColor.RED)
        return temp_id, "ERROR"

    step_start = timestamp()
    try:
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
        log(f"[{temp_id}] Mensaje eliminado de SQS en {elapsed(step_start)}", LogColor.GREEN)
    except Exception as e:
        log(f"[{temp_id}] Error al eliminar mensaje de SQS: {e}", LogColor.RED)

    step_start = timestamp()
    try:
        os.remove(temp_filename)
        log(f"[{temp_id}] Archivo temporal eliminado en {elapsed(step_start)}", LogColor.CYAN)
    except Exception as e:
        log(f"[{temp_id}] Error al eliminar archivo temporal: {e}", LogColor.RED)

    log(f"[{temp_id}] ===== FIN =====", LogColor.CYAN)
    return temp_id, "OK"

def process_messages():
    max_workers = 10
    log(f"Iniciando listener con hasta {max_workers} hilos...", LogColor.CYAN)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            while True:
                response = sqs.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=max_workers,
                    WaitTimeSeconds=10
                )
                messages = response.get('Messages', [])
                if not messages:
                    continue

                futures = [executor.submit(handle_message, msg) for msg in messages]
                for future in as_completed(futures):
                    temp_id, status = future.result()
                    if status == "OK":
                        log(f"[{temp_id}] Procesamiento COMPLETADO", LogColor.GREEN)
                    else:
                        log(f"[{temp_id}] Procesamiento FALLIDO", LogColor.RED)
        except KeyboardInterrupt:
            log("Detención por teclado. Cerrando listener...", LogColor.YELLOW)

if __name__ == "__main__":
    process_messages()
