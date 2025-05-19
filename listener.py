import time
import uuid
import os
import subprocess
import boto3
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

# URL de la cola SQS
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/153788051293/DocumentInformationQueue'

# Cliente de SQS
sqs = boto3.client('sqs', region_name='us-east-1')

def handle_message(message):
    body = message['Body']
    receipt_handle = message['ReceiptHandle']
    temp_id = str(uuid.uuid4())
    temp_filename = f"/tmp/message_{temp_id}.json"

    start_time = time.time()

    log(f"[{temp_id}] Inicia lectura de nuevo mensaje", LogColor.CYAN)
    log(body, LogColor.YELLOW)
    log(f"[{temp_id}] Mensaje recibido correctamente...", LogColor.CYAN)

    # Guardar el mensaje en un archivo temporal
    with open(temp_filename, 'w') as f:
        f.write(body)

    try:
        log(f"[{temp_id}] Ejecutando Docker con archivo de mensaje...", LogColor.YELLOW)
        subprocess.run([
            'docker', 'run', '--rm',
            '-v', f'{temp_filename}:/app/message.json',
            'my-listener-image',
            '/app/message.json'
        ], check=True)

        # Si la ejecución fue exitosa, eliminar el mensaje de la cola
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
        log(f"[{temp_id}] Mensaje eliminado de la cola SQS.", LogColor.GREEN)
        status = "OK"
    except subprocess.CalledProcessError as e:
        log(f"[{temp_id}] Error al ejecutar el contenedor Docker: {e}", LogColor.RED)
        status = "ERROR"
    finally:
        os.remove(temp_filename)
        log(f"[{temp_id}] Archivo temporal eliminado.", LogColor.CYAN)

    elapsed = time.time() - start_time
    log(f"[{temp_id}] Tiempo de procesamiento: {elapsed:.2f} segundos.", LogColor.CYAN)
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

                # Lanzar tareas en paralelo
                futures = [executor.submit(handle_message, msg) for msg in messages]

                # Esperar y loggear el estado de cada imagen
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