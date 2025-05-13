import time
import uuid
import os
import subprocess
import boto3

# URL de la cola SQS
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/153788051293/DocumentInformationQueue'

# Cliente de SQS
sqs = boto3.client('sqs', region_name='us-east-1')

# Función para recibir y procesar mensajes de SQS
def process_messages():
    print("Escuchando la cola SQS...")

    try:
        while True:
            response = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            messages = response.get('Messages', [])
            if not messages:
                continue

            for message in messages:
                body = message['Body']
                print(f"\nMensaje recibido: {body}")

                # Guardar el mensaje en un archivo temporal
                temp_id = str(uuid.uuid4())
                temp_filename = f"/tmp/message_{temp_id}.json"
                with open(temp_filename, 'w') as f:
                    f.write(body)

                try:
                    print("Ejecutando Docker con archivo de mensaje...")
                    subprocess.run([
                        'docker', 'run', '--rm',
                        '-v', f'{temp_filename}:/app/message.json',
                        'my-listener-image',
                        '/app/message.json'
                    ], check=True)
                except subprocess.CalledProcessError as e:
                    print(f"[{temp_id}] Error al ejecutar el contenedor Docker: {e}")
                finally:
                    os.remove(temp_filename)

    except KeyboardInterrupt:
        print("\nInterrupción por teclado. Saliendo...")

if __name__ == "__main__":
    process_messages()

