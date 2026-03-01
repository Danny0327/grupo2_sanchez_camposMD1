# models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from datetime import datetime
from .database import Base


class Asteroide(Base):
    __tablename__ = "asteroides"

    id = Column(Integer, primary_key=True, index=True)
    nasa_id = Column(String, index=True)
    nombre = Column(String)
    magnitud_absoluta = Column(Float)

    diametro_min_km = Column(Float)
    diametro_max_km = Column(Float)

    es_peligroso = Column(Boolean)

    fecha_aproximacion = Column(DateTime)
    velocidad_km_s = Column(Float)
    distancia_km = Column(Float)

    fecha_extraccion = Column(DateTime, default=datetime.utcnow)


class MetricasETL(Base):
    __tablename__ = "metricas_etl"

    id = Column(Integer, primary_key=True, index=True)
    fecha_ejecucion = Column(DateTime, default=datetime.utcnow)
    registros_insertados = Column(Integer)
    errores = Column(Integer)
    tiempo_ejecucion = Column(Float)
    estado = Column(String)