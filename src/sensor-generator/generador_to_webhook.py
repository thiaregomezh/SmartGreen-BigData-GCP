import time
import json
import random
import math
from datetime import datetime
import requests  # <-- Librería necesaria para enviar datos al webhook

# ==========================================
# CONFIGURACIÓN DEL SIMULADOR Y WEBHOOK
# ==========================================
INTERVALO_SEGUNDOS = 2

# Reemplaza esta URL por la de tu webhook real (ej: Make, Zapier, Node-RED, API propia, etc.)
WEBHOOK_URL = "YOUR_CLOUD_RUN_WEBHOOK_URL"

# Lista de 10 sensores distribuidos en el invernadero
LISTA_SENSORES = [
    f"INV_PLAZA_OESTE_{i:02d}" for i in range(1, 11)
]

# Inicialización del estado de cada sensor para mantener su continuidad temporal
estados_sensores = {}
for sensor_id in LISTA_SENSORES:
    estados_sensores[sensor_id] = {
        "temperatura": random.uniform(18.0, 22.0),
        "humedad": random.uniform(55.0, 65.0),
        "radiacion": 0.0
    }

def obtener_factor_solar(hora_actual):
    """
    Simula el ciclo del sol. Retorna un factor entre 0 (noche) y 1 (mediodía).
    """
    if 6 <= hora_actual <= 18:
        angulo = math.pi * (hora_actual - 6) / 12
        return math.sin(angulo)
    return 0.0

def simular_sensor_especifico(sensor_id, hora_decimal):
    """
    Genera la telemetría para un sensor específico manteniendo su estado histórico.
    """
    estado_actual = estados_sensores[sensor_id]
    factor_solar = obtener_factor_solar(hora_decimal)
    
    # Variación por sector (microclimas)
    variacion_sector = (hash(sensor_id) % 5) * 0.3  
    
    temp_objetivo = 15.0 + (12.0 * factor_solar) + (variacion_sector - 0.6)
    hum_objetivo = 75.0 - (25.0 * factor_solar) - (variacion_sector * 2)
    rad_objetivo = 800.0 * factor_solar
    
    if hash(sensor_id) % 2 == 0:
        rad_objetivo *= 0.95 

    # Continuidad temporal
    estado_actual["temperatura"] += (temp_objetivo - estado_actual["temperatura"]) * 0.05
    estado_actual["humedad"] += (hum_objetivo - estado_actual["humedad"]) * 0.05
    estado_actual["radiacion"] += (rad_objetivo - estado_actual["radiacion"]) * 0.1

    # Ruido Gaussiano
    ruido_temp = random.gauss(0, 0.2)
    ruido_hum = random.gauss(0, 0.5)
    ruido_rad = random.gauss(0, 5.0)

    temp_con_ruido = round(estado_actual["temperatura"] + ruido_temp, 2)
    hum_con_ruido = round(estado_actual["humedad"] + ruido_hum, 2)
    rad_con_ruido = round(max(0.0, estado_actual["radiacion"] + ruido_rad), 2)

    # Anomalías esporádicas (1% de probabilidad)
    if random.random() < 0.01:
        temp_con_ruido = round(temp_con_ruido + random.choice([-5.0, 7.0]), 2)
    
    # Payload JSON
    telemetria = {
        "timestamp": datetime.now().isoformat(),
        "sensor_id": sensor_id,
        "metricas": {
            "temperatura_c": temp_con_ruido,
            "humedad_relativa_porc": hum_con_ruido,
            "radiacion_solar_w_m2": rad_con_ruido
        }
    }
    
    return telemetria

# ==========================================
# BUCLE PRINCIPAL DE GENERACIÓN Y ENVÍO
# ==========================================
if __name__ == "__main__":
    print(f"🚀 Iniciando generador y envío a Webhook ({WEBHOOK_URL})...")
    print("(Presiona Ctrl+C para detener)\n")
    
    # Definimos las cabeceras estándar para enviar JSON
    headers = {'Content-Type': 'application/json'}
    
    try:
        while True:
            ahora = datetime.now()
            hora_decimal = ahora.hour + ahora.minute / 60.0
            
            print(f"=== PROCESANDO LOTE: {ahora.strftime('%Y-%m-%d %H:%M:%S')} ===")
            
            for id_sensor in LISTA_SENSORES:
                datos = simular_sensor_especifico(id_sensor, hora_decimal)
                
                # Intentamos enviar el dato individual al Webhook mediante HTTP POST
                try:
                    # json=datos convierte automáticamente el diccionario de Python a string JSON
                    # timeout=3 evita que el script se quede colgado si el webhook no responde rápido
                    respuesta = requests.post(WEBHOOK_URL, json=datos, headers=headers, timeout=3)
                    
                    if respuesta.status_code in [200, 201, 202]:
                        print(f"✅ {id_sensor}: Enviado con éxito (Status: {respuesta.status_code})")
                    else:
                        print(f"⚠️ {id_sensor}: Error en servidor receptor (Status: {respuesta.status_code})")
                        
                except requests.exceptions.Timeout:
                    print(f"❌ {id_sensor}: Tiempo de espera agotado (Timeout).")
                except requests.exceptions.ConnectionError:
                    print(f"❌ {id_sensor}: Error de conexión. ¿El webhook está activo?")
                except Exception as e:
                    print(f"❌ {id_sensor}: Error inesperado: {e}")
            
            print("=" * 50 + "\n")
            time.sleep(INTERVALO_SEGUNDOS)
            
    except KeyboardInterrupt:
        print("\n🛑 Generador y envío detenidos por el usuario.")