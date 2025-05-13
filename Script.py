import sys
import json
import uuid
import time
from datetime import datetime

start_time = time.time()
start_dt = datetime.utcnow()
execution_id = str(uuid.uuid4())

try:
    with open(sys.argv[1], 'r') as f:
        msg = json.load(f)
except Exception as e:
    print(f"[{execution_id}] Error leyendo el archivo: {e}")
    sys.exit(1)

print(f"[{execution_id}] Contenedor iniciado a las {start_dt} UTC")
print(f"Mensaje recibido desde archivo: {msg}")

end_time = time.time()
end_dt = datetime.utcnow()
duration = round(end_time - start_time, 2)

print(f"Finalizado a las {end_dt} UTC")
print(f"[{execution_id}] Duración total: {duration} segundos")

