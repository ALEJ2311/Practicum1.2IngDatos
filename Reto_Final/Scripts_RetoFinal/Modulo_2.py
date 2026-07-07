import pandas as pd
import sqlite3


def cargar_a_silver(df, nombre_tabla):
    """
    Carga un DataFrame limpio a la base de datos SQLite (Capa Silver).
    Se conecta al mismo archivo creado por el Módulo 1.
    """
    conexion = sqlite3.connect('macroentorno_silver.db')

    try:
        df.to_sql(nombre_tabla, conexion, if_exists='replace', index=False)
        print(f"Éxito: Tabla '{nombre_tabla}' cargada en macroentorno_silver.db con {len(df)} registros.")
    except Exception as e:
        print(f"Error al cargar la tabla {nombre_tabla}: {e}")
    finally:
        conexion.close()


# Rutas estáticas de los archivos
archivo_enemdu = 'data/Tabulados_Mercado_Laboral.xlsx'
archivo_mei = 'data/MEI_2018_2023.xlsx'
archivo_censo = 'data/2022_CPV_Trabajo.xlsx'

# --- 1. Limpieza de ENEMDU (Tasas) ---
print("Procesando ENEMDU...")
df_enemdu = pd.read_excel(archivo_enemdu, sheet_name='2. Tasas', skiprows=7, engine='openpyxl')

# Seleccionar primeras 8 columnas y renombrar
df_enemdu = df_enemdu.iloc[:, 0:8]
df_enemdu.columns = ['encuesta', 'periodo', 'indicador', 'total_nacional', 'area_urbana', 'area_rural', 'sexo_hombre',
                     'sexo_mujer']

# Eliminar nulos y filas con notas metodológicas al pie
df_enemdu = df_enemdu.dropna(subset=['periodo'])
df_enemdu = df_enemdu[~df_enemdu['periodo'].astype(str).str.contains('Nota')]

# Transformación de estructura pivotada a normalizada
df_enemdu_normalizado = df_enemdu.melt(
    id_vars=['encuesta', 'periodo', 'indicador', 'sexo_hombre', 'sexo_mujer'],
    value_vars=['total_nacional', 'area_urbana', 'area_rural'],
    var_name='area',
    value_name='valor_tasa'
)

# Limpieza de los nombres de la nueva columna 'area'
df_enemdu_normalizado['area'] = df_enemdu_normalizado['area'].str.replace('total_', '').str.replace('area_', '')

cargar_a_silver(df_enemdu_normalizado, 'fact_empleo_enemdu')

# --- 2. Limpieza Matriz Empleo e Ingresos (MEI) ---
print("Procesando MEI...")
df_mei = pd.read_excel(archivo_mei, sheet_name='TOTAL EMPLEO', skiprows=9, engine='openpyxl')

# Remover indicador '(p)' de los años provisionales
df_mei.columns = df_mei.columns.astype(str).str.replace(' (p)', '', regex=False)
# Estandarizar nombres de columnas
df_mei.columns = df_mei.columns.str.lower().str.replace(' ', '_').str.replace('.', '')

df_mei = df_mei.dropna(subset=['industria'])

cargar_a_silver(df_mei, 'fact_empleo_mei')

# --- 3. Limpieza Censo de Población (CPV) ---
print("Procesando Censo...")
df_censo = pd.read_excel(archivo_censo, sheet_name='1', skiprows=9, engine='openpyxl')

# Identificar columna de provincia (índice 1) para limpieza
col_provincia = df_censo.columns[1]

# Eliminar nulos y notas metodológicas al pie
df_censo = df_censo.dropna(subset=[col_provincia])
df_censo = df_censo[~df_censo[col_provincia].astype(str).str.contains('Nota')]

cargar_a_silver(df_censo, 'fact_censo_actividad')

print("\nModulo 2 finalizado. Base SQLite actualizada exitosamente.")