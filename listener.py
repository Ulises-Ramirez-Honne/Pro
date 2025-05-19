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

def handle_message(message):
    temp_id = str(uuid.uuid4())
    temp_filename = f"/tmp/message_{temp_id}.json"
    start_time = time.time()

    # Solo guarda el body (que es un string JSON)
    body = message['Body']
    try:
        # Validar que el body es un JSON válido antes de guardar
        json.loads(body)
        with open(temp_filename, 'w') as f:
            f.write(body)
    except Exception as e:
        log(f"[{temp_id}] Body no es JSON válido: {e}", LogColor.RED)
        return temp_id, "ERROR"

    # Log compacto y ordenado
    log_lines = [
        f"[{temp_id}] Inicia procesamiento",
        f"[{temp_id}] Mensaje recibido correctamente...",
        f"[{temp_id}] Ejecutando Docker con archivo de mensaje..."
    ]
    log('\n'.join(log_lines), LogColor.CYAN)

    status = "OK"
    try:
        subprocess.run([
            'docker', 'run', '--rm',
            '-v', f'{temp_filename}:/app/message.json',
            'my-listener-image',
            '/app/message.json'
        ], check=True)

        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
        log(f"[{temp_id}] Mensaje eliminado de la cola SQS.", LogColor.GREEN)
    except subprocess.CalledProcessError as e:
        log(f"[{temp_id}] Error al ejecutar el contenedor Docker: {e}", LogColor.RED)
        status = "ERROR"
    finally:
        os.remove(temp_filename)
        log(f"[{temp_id}] Archivo temporal eliminado.", LogColor.CYAN)

    elapsed = time.time() - start_time
    log(f"[{temp_id}] Tiempo de procesamiento: {elapsed:.2f} segundos.", LogColor.YELLOW)
    return temp_id, status

def process_messages():
    log("Escuchando la cola SQS...", LogColor.CYAN)
    max_workers = 10
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
                        log(f"[{temp_id}] Procesamiento finalizado correctamente.", LogColor.GREEN)
                    else:
                        log(f"[{temp_id}] Procesamiento fallido.", LogColor.RED)

        except KeyboardInterrupt:
            log("\nInterrupción por teclado. Saliendo...", LogColor.YELLOW)

if __name__ == "__main__":
    process_messages()