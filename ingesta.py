import mysql.connector
import csv
import boto3

# --- Configuración MySQL ---
db_config = {
    "host": "localhost",       # cambia por tu host
    "user": "root",            # cambia por tu usuario
    "password": "utec",    # cambia por tu password
    "database": "bd_api_employees"      # cambia por tu base de datos
}
tabla = "employees"             # cambia por tu tabla

# --- Configuración S3 ---
ficheroUpload = "data_employees.csv"
nombreBucket = "gcr-output-01"

# --- Conexión MySQL ---
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Leer todos los registros de la tabla
cursor.execute(f"SELECT * FROM {tabla}")
rows = cursor.fetchall()
column_names = [desc[0] for desc in cursor.description]

# Guardar en CSV
with open(ficheroUpload, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(column_names)  # cabecera
    writer.writerows(rows)

cursor.close()
conn.close()

# --- Subir a S3 ---
s3 = boto3.client('s3')
s3.upload_file(ficheroUpload, nombreBucket, ficheroUpload)

print("Ingesta completada")
