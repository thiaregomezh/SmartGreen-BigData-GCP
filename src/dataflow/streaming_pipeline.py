import argparse
import json
import logging
import statistics
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions, PipelineOptions, StandardOptions)
from apache_beam.transforms import window

# Rangos físicos plausibles (respaldo de la detección estadística).
RANGOS = {
    "temperatura": (-5.0, 60.0),
    "humedad": (0.0, 100.0),
    "radiacion": (0.0, 1500.0),
}
Z_UMBRAL = 3.0       # nº de desviaciones estándar para marcar outlier
MIN_MUESTRAS = 5     # mínimo de lecturas en la ventana para usar z-score


def parse_and_clean(raw_bytes):
    #Parsea el JSON de Pub/Sub y lo normaliza a un dict plano interno.

    # Acepta el formato generado por el simulador de sensores:
      {timestamp, sensor_id, metricas:{temperatura_c, humedad_relativa_porc, radiacion_solar_w_m2}}
 
    try:
        msg = json.loads(raw_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return
    if not isinstance(msg, dict):
        return

    metricas = msg.get("metricas", {})
    data = {
        "sensor_id": msg.get("sensor_id"),
        "timestamp_evento": msg.get("timestamp_evento") or msg.get("timestamp"),
        "temperatura": msg.get("temperatura", metricas.get("temperatura_c")),
        "humedad": msg.get("humedad", metricas.get("humedad_relativa_porc")),
        "radiacion": msg.get("radiacion", metricas.get("radiacion_solar_w_m2")),
    }

    if data["sensor_id"] is None or data["timestamp_evento"] is None:
        return
    if any(data[v] is None for v in RANGOS):
        return

    try:
        ts = str(data["timestamp_evento"]).replace("Z", "+00:00")
        datetime.fromisoformat(ts)
        for v in RANGOS:
            data[v] = float(data[v])
    except (ValueError, TypeError):
        return

    data["timestamp_evento"] = ts
    yield data


def to_raw_row(r):
    return {
        "timestamp_evento": r["timestamp_evento"],
        "sensor_id": r["sensor_id"],
        "temperatura": r["temperatura"],
        "humedad": r["humedad"],
        "radiacion": r["radiacion"],
        "is_outlier": r["is_outlier"],
        "outlier_motivo": r["outlier_motivo"],
        "timestamp_ingesta": r["timestamp_ingesta"],
    }


class DetectarYAgregar(beam.DoFn):
    #Por (sensor, ventana): detecta outliers (z-score + rango) y agrega.

    #Emite dos salidas etiquetadas:
      # 'raw': cada lectura enriquecida con is_outlier/outlier_motivo.
      # 'agg': una fila de promedios de 1 min (excluyendo outliers).


    def process(self, element, win=beam.DoFn.WindowParam):
        sensor_id, lecturas = element
        lecturas = list(lecturas)
        n = len(lecturas)
        ahora = datetime.now(timezone.utc).isoformat()

        # Media y desviación estándar por variable dentro de la ventana.
        stats = {}
        for var in RANGOS:
            valores = [r[var] for r in lecturas]
            media = statistics.fmean(valores)
            desv = statistics.pstdev(valores) if n >= 2 else 0.0
            stats[var] = (media, desv)

        # Marcado de cada lectura.
        for r in lecturas:
            motivos = []
            for var, (lo, hi) in RANGOS.items():
                if not (lo <= r[var] <= hi):
                    motivos.append(f"{var}_rango")
            if n >= MIN_MUESTRAS:
                for var in RANGOS:
                    media, desv = stats[var]
                    if desv > 1e-9 and abs(r[var] - media) / desv > Z_UMBRAL:
                        motivos.append(f"{var}_zscore")
            r["is_outlier"] = bool(motivos)
            r["outlier_motivo"] = ",".join(sorted(set(motivos))) if motivos else None
            r["timestamp_ingesta"] = ahora
            yield beam.pvalue.TaggedOutput("raw", to_raw_row(r))

        # Promedios móviles excluyendo outliers (no contaminan la media).
        validas = [r for r in lecturas if not r["is_outlier"]]
        m = len(validas)
        avg = lambda f: round(sum(r[f] for r in validas) / m, 2) if m else None
        yield beam.pvalue.TaggedOutput("agg", {
            "ventana": win.start.to_utc_datetime().isoformat(),
            "sensor_id": sensor_id,
            "temperatura_promedio": avg("temperatura"),
            "humedad_promedio": avg("humedad"),
            "radiacion_promedio": avg("radiacion"),
            "n_lecturas": m,
            "n_outliers": sum(1 for r in lecturas if r["is_outlier"]),
        })


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dataset", default="agro_dataset")
    parser.add_argument("--project", required=True)
    known, beam_args = parser.parse_known_args(argv)

    opts = PipelineOptions(beam_args)
    opts.view_as(StandardOptions).streaming = True
    opts.view_as(GoogleCloudOptions).project = known.project

    raw_table = f"{known.project}:{known.dataset}.sensores_raw"
    agg_table = f"{known.project}:{known.dataset}.sensores_promedio_1min"

    with beam.Pipeline(options=opts) as p:
        grupos = (
            p
            | "LeerPubSub" >> beam.io.ReadFromPubSub(subscription=known.subscription)
            | "ParsearLimpiar" >> beam.FlatMap(parse_and_clean)
            | "Ventana1min" >> beam.WindowInto(window.FixedWindows(60))
            | "PorSensor" >> beam.Map(lambda r: (r["sensor_id"], r))
            | "Agrupar" >> beam.GroupByKey()
        )

        salidas = grupos | "DetectarAgregar" >> beam.ParDo(
            DetectarYAgregar()).with_outputs("raw", "agg")

        (salidas.raw | "EscribirRaw" >> beam.io.WriteToBigQuery(
            raw_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

        (salidas.agg | "EscribirAgg" >> beam.io.WriteToBigQuery(
            agg_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
