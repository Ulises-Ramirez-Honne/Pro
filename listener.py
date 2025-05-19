import time
import uuid
import os
import subprocess
import boto3
import json
import re
from concurrent.futures import ThreadPoolExecutor
import threading

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

running_containers = 0
lock = threading.Lock()
MAX_CONTAINERS = 5  # Límite máximo de contenedores simultáneos

def increment_containers():
    global running_containers
    with lock:
        while running_containers >= MAX_CONTAINERS:
            log(f"[WAIT] Esperando... Contenedores en uso: {running_containers}/{MAX_CONTAINERS}", LogColor.YELLOW)
            time.sleep(1)
        running_containers += 1
        return running_containers

def decrement_containers():
    global running_containers
    with lock:
        running_containers -= 1
        return running_containers

def timestamp():
    return time.time()

def elapsed(start):
    return f"{time.time() - start:.2f} segundos"

def extract_json_from_body(body):
    match = re.search(r'(\{.*\})', body, re.DOTALL)
    if match:
        return match.group(1)
    else:
        raise ValueError("No se encontró un JSON válido en el cuerpo del mensaje")

def run_docker_async(temp_filename, temp_id, receipt_handle):
    step_start = timestamp()
    count = increment_containers()
    log(f"[{temp_id}] Ejecutando contenedor Docker... Contenedores simultáneos: {count}", LogColor.YELLOW)
    try:
        proc = subprocess.Popen([
            'docker', 'run', '--rm',
            '-v', f'{temp_filename}:/app/message.json',
            'my-listener-image',
            '/app/message.json'
        ])

        def watchdog():
            time.sleep(10)
            if proc.poll() is None:
                log(f"[{temp_id}] ADVERTENCIA: El contenedor lleva más de 10 segundos en ejecución", LogColor.YELLOW)

        threading.Thread(target=watchdog, daemon=True).start()

        proc.wait()
        count = decrement_containers()
        log(f"[{temp_id}] Docker ejecutado correctamente en {elapsed(step_start)}. Contenedores simultáneos: {count}", LogColor.GREEN)

        try:
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)
            log(f"[{temp_id}] Mensaje eliminado de SQS", LogColor.GREEN)
        except Exception as e:
            log(f"[{temp_id}] Error al eliminar mensaje de SQS: {e}", LogColor.RED)

        try:
            os.remove(temp_filename)
            log(f"[{temp_id}] Archivo temporal eliminado", LogColor.CYAN)
        except Exception as e:
            log(f"[{temp_id}] Error al eliminar archivo temporal: {e}", LogColor.RED)

    except Exception as e:
        count = decrement_containers()
        log(f"[{temp_id}] Error en ejecución de Docker: {e}. Contenedores simultáneos: {count}", LogColor.RED)

def handle_message(message):
    temp_id = str(uuid.uuid4())
    temp_filename = f"/tmp/message_{temp_id}.json"

    log(f"[{temp_id}] ===== INICIO =====", LogColor.CYAN)
    step_start = timestamp()
    body = message['Body']
    try:
        body_json = extract_json_from_body(body)
        json.loads(body_json)
        with open(temp_filename, 'w') as f:
            f.write(body_json)
        log(f"[{temp_id}] JSON guardado en {elapsed(step_start)}", LogColor.GREEN)
    except Exception as e:
        log(f"[{temp_id}] Error al guardar JSON: {e}", LogColor.RED)
        return temp_id, "ERROR"

    threading.Thread(target=run_docker_async, args=(temp_filename, temp_id, message['ReceiptHandle'])).start()
    log(f"[{temp_id}] Contenedor lanzado en hilo separado, no bloqueo hilo principal", LogColor.YELLOW)
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
                for future in futures:
                    temp_id, status = future.result()
                    if status == "OK":
                        log(f"[{temp_id}] Procesamiento INICIADO correctamente", LogColor.GREEN)
                    else:
                        log(f"[{temp_id}] Procesamiento FALLIDO en inicialización", LogColor.RED)
        except KeyboardInterrupt:
            log("Detención por teclado. Cerrando listener...", LogColor.YELLOW)

if __name__ == "__main__":
    process_messages()
