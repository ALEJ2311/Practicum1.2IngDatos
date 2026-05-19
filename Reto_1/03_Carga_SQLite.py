import pandas as pd
from sqlalchemy import create_engine

# 1. Configuración de rutas
archivo_limpio = 'data/MINEDUC_Limpio.csv'
db_url = 'sqlite:///amie_mineduc.db'


# Cargar el dataset limpio generado en el Paso 2
try:
    df = pd.read_csv(archivo_limpio, sep=';', encoding='latin-1')
    print(f"Dataset limpio cargado correctamente ({df.shape[0]} registros).")
except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{archivo_limpio}'. Ejecuta primero el Paso 2.")
    exit()

# 2. Conexión y Carga en SQLite
engine = create_engine(db_url)

# Filtrado preventivo por el año lectivo actual (2023-2024)
df_reciente = df[df['anio_lectivo'] == '2023-2024'].copy()

# Si por alguna variación del CSV viene vacío el filtro, usamos el dataframe completo
if df_reciente.empty:
    df_reciente = df.copy()

# Guardar en la tabla 'instituciones' reemplazándola si ya existe
df_reciente.to_sql('instituciones', engine, if_exists='replace', index=False)
print("Datos cargados en la tabla 'instituciones' dentro de 'amie_mineduc.db'.")
print("-" * 50)

# Ejecutamos las consultas de validación, antes de mandar a PowerBI
print("Consultas Pre Dashbord:")
print("-" * 50)
# Consulta 1: Matrícula Nacional por Provincia (Ordenado de mayor a menor)
query_p1 = """
SELECT provincia, SUM(total_estudiantes) AS matricula_total
FROM instituciones
GROUP BY provincia
ORDER BY matricula_total DESC;
"""
print("\nPregunta 1: Top 5 Provincias con Mayor Matrícula Total:")
df_p1 = pd.read_sql_query(query_p1, engine)
print(df_p1.head(5))  # Mostramos las primeras 5
print("-" * 50)
# Consulta 2: Sostenimiento por Área en la provincia de LOJA
query_p2 = """
SELECT area, sostenimiento, COUNT(cod_amie) AS total_instituciones
FROM instituciones
WHERE provincia = 'LOJA' 
  AND sostenimiento IN ('Fiscal', 'Particular')
GROUP BY area, sostenimiento
ORDER BY area, total_instituciones DESC;
"""
print("\nPregunta 2: Distribución de Sostenimiento por Área en Loja (Fiscal vs Particular) :")
df_p2 = pd.read_sql_query(query_p2, engine)
print(df_p2)
print("-" * 50)
