import pandas as pd
archivo_csv = 'data/2_MINEDUC_RegistrosAdministrativos_2023-2024Inicio.csv'
print("           01: Exploración de Datos")

try:
    df = pd.read_csv(archivo_csv, sep=';', encoding='latin-1')
    print(" Archivo cargado con éxito para exploración \n")
except FileNotFoundError:
    print(f"-> Error: No se encontró el archivo en la ruta '{archivo_csv}'")
    exit()

# Eliminamos espacios en blanco invisibles al inicio o final de los nombres de las columnas
df.columns = df.columns.str.strip()

# 1. Dimensiones del Dataset
print("1. Dataset (Filas, Columnas):")
print(df.shape)
print("-" * 50)

# 2. Cantidad de Nulos por Columna (Top 15)
print("2. Cantidad de valores nulos por columna (15):")
print(df.isnull().sum().sort_values(ascending=False).head(15))
print("-" * 50)

# 3. Distribución del Campo Categórico: Sostenimiento
print("3. Distribución del campo categórico 'Sostenimiento':")
print(df['Sostenimiento'].value_counts())
print("-" * 50)

# 4. Distribución del Campo Categórico: Nivel Educación
print("4. Distribución del campo Categórico 'Nivel de Educación':")
print(df['Nivel Educación'].value_counts())
