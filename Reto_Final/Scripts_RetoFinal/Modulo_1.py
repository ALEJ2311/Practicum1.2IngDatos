import pandas as pd
import sqlite3


def cargar_a_silver(df, nombre_tabla):
    """
    Carga un DataFrame limpio a la base de datos SQLite (Capa Silver).
    Si el archivo .db no existe, SQLite lo crea automáticamente.
    """
    conexion = sqlite3.connect('macroentorno_silver.db')

    try:
        df.to_sql(nombre_tabla, conexion, if_exists='replace', index=False)
        print(f"Éxito: Tabla '{nombre_tabla}' cargada en macroentorno_silver.db con {len(df)} registros.")
    except Exception as e:
        print(f"Error al cargar la tabla {nombre_tabla}: {e}")
    finally:
        conexion.close()


# Rutas estáticas de los archivos fuente
archivo_pib = 'data/PIB.xlsx'
archivo_iee = 'data/IEE.xlsx'
archivo_petroleo = 'data/PETRÓLEO.xlsx'
archivo_riesgo = 'data/RIESGO PAÍS.xlsx'
archivo_vab = 'data/VAB 2018-2023.xlsx'

# --- 1. Limpieza del PIB ---
print("Procesando PIB...")
df_pib = pd.read_excel(archivo_pib, engine='openpyxl')

df_pib = df_pib.rename(columns={
    'AÑO': 'anio',
    'PIB 2018 = 100': 'pib_real_indice',
    'VAR ANUAL PIB': 'variacion_anual_pib',
    'PIB PER CÁPITA NOMINAL': 'pib_per_capita_nominal'
})

df_pib = df_pib[['anio', 'pib_real_indice', 'variacion_anual_pib', 'pib_per_capita_nominal']]
df_pib = df_pib.dropna(subset=['anio'])

cargar_a_silver(df_pib, 'fact_macro_anual')

# --- 2. Limpieza de Expectativas (IEE) ---
print("Procesando IEE...")
df_iee = pd.read_excel(archivo_iee, skiprows=7, engine='openpyxl')

df_iee = df_iee.dropna(subset=['Fecha'])
df_iee['Fecha'] = pd.to_datetime(df_iee['Fecha'], errors='coerce')
df_iee = df_iee.dropna(subset=['Fecha'])

cargar_a_silver(df_iee, 'fact_expectativas_iee')

# --- 3. Limpieza de Petróleo WTI ---
print("Procesando Petróleo WTI...")
df_petroleo = pd.read_excel(archivo_petroleo, skiprows=1, engine='openpyxl')

df_petroleo = df_petroleo.rename(columns={'Período': 'fecha', 'Precio Petróleo (WTI) en USD por barril': 'precio_wti'})
df_petroleo = df_petroleo.dropna(subset=['fecha'])
df_petroleo['fecha'] = pd.to_datetime(df_petroleo['fecha'])

cargar_a_silver(df_petroleo, 'fact_petroleo_diario')

# --- 4. Limpieza de Riesgo País ---
print("Procesando Riesgo País...")
df_riesgo = pd.read_excel(archivo_riesgo, skiprows=1, engine='openpyxl')

df_riesgo = df_riesgo.rename(columns={'Período': 'fecha', 'Riesgo País en Puntos Básicos': 'riesgo_pais_pb'})
df_riesgo = df_riesgo.dropna(subset=['fecha'])
df_riesgo['fecha'] = pd.to_datetime(df_riesgo['fecha'])

cargar_a_silver(df_riesgo, 'fact_riesgo_diario')

# --- 5. Limpieza de VAB Cantonal / Provincial ---
print("Procesando VAB Cantonal...")
df_vab = pd.read_excel(archivo_vab, sheet_name='DATA', engine='openpyxl')

df_vab = df_vab.rename(columns={'AÑO': 'anio'})
df_vab.columns = df_vab.columns.str.lower().str.replace(' ', '_').str.replace('ó', 'o')
df_vab = df_vab.dropna(subset=['anio'])

cargar_a_silver(df_vab, 'fact_vab_geografico')

print("\nModulo 1 finalizado. Base SQLite creada/actualizada exitosamente.")