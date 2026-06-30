import pandas as pd

# Rutas de los archivos
archivo_pib = 'data/PIB.xlsx'
archivo_iee = 'data/IEE.xlsx'
archivo_petroleo = 'data/PETRÓLEO.xlsx'
archivo_riesgo = 'data/RIESGO PAÍS.xlsx'
archivo_vab = 'data/VAB 2018-2023.xlsx'

# --- 1. Limpieza del PIB ---
print("Procesando PIB...")
df_pib = pd.read_excel(archivo_pib, engine='openpyxl')

# Renombrar columnas
df_pib = df_pib.rename(columns={
    'AÑO': 'anio',
    'PIB 2018 = 100': 'pib_real_indice',
    'VAR ANUAL PIB': 'variacion_anual_pib',
    'PIB PER CÁPITA NOMINAL': 'pib_per_capita_nominal'
})

df_pib = df_pib[['anio', 'pib_real_indice', 'variacion_anual_pib', 'pib_per_capita_nominal']]
df_pib = df_pib.dropna(subset=['anio'])

print(f"PIB listo: {df_pib.shape}")


# --- 2. Limpieza de Expectativas (IEE) ---
print("Procesando IEE...")
df_iee = pd.read_excel(archivo_iee, skiprows=7, engine='openpyxl')

df_iee = df_iee.dropna(subset=['Fecha'])
# Convertir a fecha (los textos al pie quedarán como NaT)
df_iee['Fecha'] = pd.to_datetime(df_iee['Fecha'], errors='coerce')
df_iee = df_iee.dropna(subset=['Fecha'])

print(f"IEE listo: {df_iee.shape}")


# --- 3. Limpieza de Petróleo WTI ---
print("Procesando Petróleo WTI...")
df_petroleo = pd.read_excel(archivo_petroleo, skiprows=1, engine='openpyxl')

df_petroleo = df_petroleo.rename(columns={'Período': 'fecha', 'Precio Petróleo (WTI) en USD por barril': 'precio_wti'})
df_petroleo = df_petroleo.dropna(subset=['fecha'])
df_petroleo['fecha'] = pd.to_datetime(df_petroleo['fecha'])

print(f"Petróleo listo: {df_petroleo.shape}")


# --- 4. Limpieza de Riesgo País ---
print("Procesando Riesgo País...")
df_riesgo = pd.read_excel(archivo_riesgo, skiprows=1, engine='openpyxl')

df_riesgo = df_riesgo.rename(columns={'Período': 'fecha', 'Riesgo País en Puntos Básicos': 'riesgo_pais_pb'})
df_riesgo = df_riesgo.dropna(subset=['fecha'])
df_riesgo['fecha'] = pd.to_datetime(df_riesgo['fecha'])

print(f"Riesgo País listo: {df_riesgo.shape}")


# --- 5. Limpieza de VAB Cantonal / Provincial ---
print("Procesando VAB Cantonal...")
df_vab = pd.read_excel(archivo_vab, sheet_name='DATA', engine='openpyxl')

df_vab = df_vab.rename(columns={'AÑO': 'anio'})
df_vab.columns = df_vab.columns.str.lower().str.replace(' ', '_').str.replace('ó', 'o')
df_vab = df_vab.dropna(subset=['anio'])

print(f"VAB listo: {df_vab.shape}")

print("\nModulo 1 finalizado. Datos en memoria.")