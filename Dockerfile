#Usamos una imagen base de Python 3.9
FROM python:3.9-slim

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Instalar las dependencias del sistema necesarias para OpenCV
RUN apt-get update && apt-get install -y \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar las dependencias de Python, incluyendo boto3 y opencv-python
RUN pip install --no-cache-dir boto3 opencv-python pillow colorama

# Copiar el archivo de tu script Python al contenedor
COPY Script.py /app/

# Ejecutar el script con un parámetro (cuando se inicie el contenedor)
ENTRYPOINT ["python3", "Script.py"]

