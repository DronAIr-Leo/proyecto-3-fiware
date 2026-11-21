"""Simula lecturas de los tres tipos de sensor del Proyecto 3 y las carga en InfluxDB.
Modelo (apartado 6): un measurement por tipo de sensor, tag sensor_id, fields = magnitudes medidas."""
import os
import random
from datetime import datetime, timedelta, timezone

from influxdb_client_3 import InfluxDBClient3, Point

HOST = os.environ.get("INFLUX_HOST", "http://localhost:8181")
TOKEN = os.environ.get("INFLUX_TOKEN", "sin-auth")  # con --without-auth vale cualquiera
cliente = InfluxDBClient3(host=HOST, database="smartcity", token=TOKEN)

SENSORES_TEMP_HUM = ["temp-001", "temp-002", "temp-003"]
SENSORES_CO2 = ["co2-001", "co2-002"]
SENSORES_AGUA = ["agua-001", "agua-002"]


def lectura_temp_hum(sensor_id: str, instante: datetime) -> Point:
    return (
        Point("temperatura_humedad")
        .tag("sensor_id", sensor_id)
        .field("temperatura", round(random.uniform(18, 32), 1))
        .field("humedad", round(random.uniform(30, 70), 1))
        .time(instante)
    )


def lectura_co2(sensor_id: str, instante: datetime) -> Point:
    return (
        Point("co2")
        .tag("sensor_id", sensor_id)
        .field("co2_ppm", random.randint(400, 1200))
        .time(instante)
    )


def lectura_agua(sensor_id: str, instante: datetime) -> Point:
    return (
        Point("calidad_agua")
        .tag("sensor_id", sensor_id)
        .field("ph", round(random.uniform(6.5, 8.5), 2))
        .field("temperatura", round(random.uniform(12, 24), 1))
        .field("cloro", round(random.uniform(0.2, 1.0), 2))
        .time(instante)
    )


if __name__ == "__main__":
    ahora = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    instantes = [ahora - timedelta(minutes=i) for i in range(60)]  # última hora, una lectura por minuto

    puntos = (
        [lectura_temp_hum(s, t) for s in SENSORES_TEMP_HUM for t in instantes]
        + [lectura_co2(s, t) for s in SENSORES_CO2 for t in instantes]
        + [lectura_agua(s, t) for s in SENSORES_AGUA for t in instantes]
    )
    cliente.write(record=puntos)  # escritura en lote
    print(f"temperatura_humedad: {len(SENSORES_TEMP_HUM) * 60} puntos")
    print(f"co2:                 {len(SENSORES_CO2) * 60} puntos")
    print(f"calidad_agua:        {len(SENSORES_AGUA) * 60} puntos")
    print(f"Simulación completada: {len(puntos)} puntos.")
    cliente.close()