# SmartGreen Big Data GCP 🌱☁️

Sistema de procesamiento de datos IoT en tiempo real desarrollado como proyecto académico utilizando **Google Cloud Platform (GCP)** y **Apache Beam**.

## Descripción

SmartGreen es una solución de análisis de datos en streaming para un invernadero inteligente. El sistema simula sensores IoT que generan mediciones de temperatura, humedad y radiación solar, las cuales son procesadas en tiempo real mediante una arquitectura basada en servicios de Google Cloud.

El pipeline realiza la limpieza de datos, detección de valores atípicos (outliers), agregación por ventanas de tiempo y almacenamiento de los resultados en BigQuery para su posterior visualización mediante dashboards.

## Funcionalidades

- Simulación de sensores IoT.
- Ingesta de datos mediante Webhook.
- Publicación de eventos en Google Cloud Pub/Sub.
- Procesamiento en streaming con Apache Beam y Google Cloud Dataflow.
- Limpieza y validación de datos.
- Detección de outliers mediante rangos físicos y Z-Score.
- Agregación de datos en ventanas de 1 minuto.
- Almacenamiento de datos en BigQuery.
- Visualización de resultados mediante dashboard.

## Arquitectura

Sensores IoT → Webhook → Pub/Sub → Apache Beam (Dataflow) → BigQuery → Dashboard

## Tecnologías utilizadas

### Lenguaje
- Python

### Cloud & Big Data
- Google Cloud Platform (GCP)
- Cloud Run
- Pub/Sub
- Apache Beam
- Google Cloud Dataflow
- BigQuery


##  Instalación

Clonar el repositorio:

```bash
git clone https://github.com/thiaregomezh/SmartGreen-BigData-GCP.git
```

Instalar las dependencias necesarias en cada módulo utilizando su correspondiente archivo `requirements.txt`.

## 🎓 Contexto académico

Proyecto desarrollado durante la carrera de **Ingeniería en Informática** en **Duoc UC**, aplicando conceptos de procesamiento de datos en tiempo real, Data Engineering y computación en la nube.

##  Autora

**Thiare Gómez Herrera**

Estudiante de Ingeniería en Informática - Duoc UC
