import pandas as pd

# Rutas de los archivos
archivo_companias = 'data/directorio_companias.xlsx'
archivo_ciiu = 'data/bi_ciiu.csv'
archivo_ranking = 'data/bi_ranking.csv'

# --- 1. Limpieza del Directorio de Compañías ---
print("Procesando Directorio de Compañías...")
# Nota: La lectura de este archivo toma tiempo debido al volumen de datos
df_companias = pd.read_excel(archivo_companias, skiprows=4, engine='openpyxl')

# Estandarización de columnas
df_companias.columns = (df_companias.columns.str.lower()
                        .str.replace(' ', '_')
                        .str.replace('ó', 'o')
                        .str.replace('í', 'i')
                        .str.replace('ñ', 'n'))

# Eliminar registros sin RUC
df_companias = df_companias.dropna(subset=['ruc'])

# Transformación de variable a booleana
if 'presento_balance_inicial' in df_companias.columns:
    df_companias['presento_balance_inicial'] = df_companias['presento_balance_inicial'].map({
        'SI': True,
        'NO': False,
        'NO APLICA': None
    })

print(f"Compañías listo: {df_companias.shape}")


# --- 2. Limpieza Clasificador CIIU ---
print("Procesando CIIU...")
df_ciiu = pd.read_csv(archivo_ciiu)

# Estandarización de columnas
df_ciiu.columns = df_ciiu.columns.str.lower().str.replace(' ', '_')

print(f"CIIU listo: {df_ciiu.shape}")


# --- 3. Limpieza del Ranking Empresarial ---
print("Procesando Ranking Empresarial...")
df_ranking = pd.read_csv(archivo_ranking, low_memory=False)

# Renombrar columna de año y estandarizar el resto
if 'AÑO' in df_ranking.columns:
    df_ranking = df_ranking.rename(columns={'AÑO': 'anio'})

df_ranking.columns = df_ranking.columns.str.lower().str.replace(' ', '_')

# Eliminar registros sin RUC
if 'ruc' in df_ranking.columns:
    df_ranking = df_ranking.dropna(subset=['ruc'])

print(f"Ranking listo: {df_ranking.shape}")

print("\nModulo 3 finalizado. Datos en memoria.")