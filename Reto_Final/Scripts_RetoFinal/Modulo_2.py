import pandas as pd

# Rutas de los archivos
archivo_enemdu = 'data/Tabulados_Mercado_Laboral.xlsx'
archivo_mei = 'data/MEI_2018_2023.xlsx'
archivo_censo = 'data/2022_CPV_Trabajo.xlsx'

# --- 1. Limpieza de ENEMDU (Tasas) ---
print("Procesando ENEMDU...")
df_enemdu = pd.read_excel(archivo_enemdu, sheet_name='2. Tasas', skiprows=7, engine='openpyxl')

# Seleccionar primeras 8 columnas y renombrar
df_enemdu = df_enemdu.iloc[:, 0:8]
df_enemdu.columns = ['encuesta', 'periodo', 'indicador', 'total_nacional', 'area_urbana', 'area_rural', 'sexo_hombre', 'sexo_mujer']

# Eliminar nulos y filas con notas metodológicas al pie
df_enemdu = df_enemdu.dropna(subset=['periodo'])
df_enemdu = df_enemdu[~df_enemdu['periodo'].astype(str).str.contains('Nota')]

print(f"ENEMDU listo: {df_enemdu.shape}")


# --- 2. Limpieza Matriz Empleo e Ingresos (MEI) ---
print("Procesando MEI...")
df_mei = pd.read_excel(archivo_mei, sheet_name='TOTAL EMPLEO', skiprows=9, engine='openpyxl')

# Remover indicador '(p)' de los años provisionales
df_mei.columns = df_mei.columns.astype(str).str.replace(' (p)', '', regex=False)
# Estandarizar nombres de columnas
df_mei.columns = df_mei.columns.str.lower().str.replace(' ', '_').str.replace('.', '')

df_mei = df_mei.dropna(subset=['industria'])

print(f"MEI listo: {df_mei.shape}")


# --- 3. Limpieza Censo de Población (CPV) ---
print("Procesando Censo...")
df_censo = pd.read_excel(archivo_censo, sheet_name='1', skiprows=9, engine='openpyxl')

# Identificar columna de provincia (índice 1) para limpieza
col_provincia = df_censo.columns[1]

# Eliminar nulos y notas metodológicas al pie
df_censo = df_censo.dropna(subset=[col_provincia])
df_censo = df_censo[~df_censo[col_provincia].astype(str).str.contains('Nota')]

print(f"Censo listo: {df_censo.shape}")

print("\nModulo 2 finalizado. Datos en memoria.")