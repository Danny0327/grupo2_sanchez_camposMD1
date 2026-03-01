# 🌤️ Proyecto ETL Clima

## Descripción
Este proyecto realiza un flujo **ETL (Extract, Transform, Load)** para extraer datos del clima de diferentes ciudades usando la API de Weatherstack, almacenar la información en una base de datos PostgreSQL y generar visualizaciones.  

Flujo completo:

- **Extracción:** Consulta de datos meteorológicos desde la API.  
- **Transformación:** Procesamiento y limpieza de los datos.  
- **Carga:** Guardado de registros en base de datos.  
- **Visualización:** Gráficos para análisis de clima.  
- **Métricas ETL:** Registro de métricas de ejecución (ciudades procesadas, errores, tiempo total, estado).

---

## 📁 Estructura del Proyecto

Trabajos/
├── Etl_nicial/
│ ├── extractor.py # Extrae y procesa los datos de la API
│ ├── transformador.py # (Opcional, procesamiento adicional)
│ └── visualizador.py # Genera gráficas de análisis
├── scripts/
│ ├── database.py # Conexión y configuración de la BD
│ ├── models.py # Modelos SQLAlchemy (Tablas)
│ └── create_tables.py # Crear tablas en la base de datos
├── data/ # Resultados: CSV, JSON, PNG
├── logs/ # Registros de ejecución
├── .env # Variables de entorno (API, DB, ciudades)
├── requirements.txt # Dependencias Python
└── README.md # Documentación

---

## Configuración del Entorno

1. Clonar el repositorio:

git clone <URL_DEL_REPOSITORIO>
cd Trabajos

python -m venv venv
.\venv\Scripts\Activate.ps1

pip install -r requirements.txt


🛠️ Inicialización de la Base de Datos
python scripts/create_tables.py

Tablas creadas:

Tabla	Descripción
ciudades	Información de cada ciudad
registros_clima	Datos meteorológicos extraídos
metricas_etl	Métricas del proceso ETL

## Configuración del Entorno
Uso del Proyecto
1️⃣ Ejecutar Extracción Manual
python Etl_nicial/extractor.py

Extrae datos de la API.

Procesa y guarda registros en BD.

Exporta CSV y JSON en data/.

2️⃣ Visualización de Datos
python Etl_nicial/visualizador.py

Genera gráficos de temperatura, humedad, viento y sensación térmica.

Archivo generado: data/clima_analysis.png.

3️⃣ Scheduler Automático
python scripts/scheduler.py

Automatiza ejecución ETL cada 10 segundos (configurable).

Registra logs en logs/etl.log.

## Salidas del Proyecto
Archivo	Descripción
data/clima.csv	Datos tabulares en CSV
data/clima_raw.json	Datos en formato JSON
data/clima_analysis.png	Gráficas de análisis de clima.

