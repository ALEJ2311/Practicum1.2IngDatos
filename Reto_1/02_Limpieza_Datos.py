import pandas as pd

# 1. Configuración de rutas
archivo_original = 'data/2_MINEDUC_RegistrosAdministrativos_2023-2024Inicio.csv'
archivo_limpio = 'data/MINEDUC_Limpio.csv'


# Carga del dataset con la configuración
df = pd.read_csv(archivo_original, sep=';', encoding='latin-1')
df.columns = df.columns.str.strip()
print(f" Dataset original cargado. Registros iniciales: {df.shape[0]}")
print("-" * 50)

# Transformación 1: Renombrar Columnas (Mapeo Completo y Adaptado)

RENAME = {
    'Año lectivo': 'anio_lectivo',
    'AMIE': 'cod_amie',
    'Nombre_Institución': 'nombre_institucion',
    'Nivel Educación': 'nivel_educacion',
    'Sostenimiento': 'sostenimiento',
    'Área': 'area',
    'Modalidad': 'modalidad',
    'Jornada': 'jornada',
    'Provincia': 'provincia',
    'Cantón': 'canton',
    'Parroquia': 'parroquia',
    'Total Docentes': 'total_docentes',
    'Total Estudiantes': 'total_estudiantes',
    'Estudiantes Femenino': 'estudiantes_f',
    'Estudiantes Masculino': 'estudiantes_m'
}

df = df.rename(columns=RENAME)
print("Transformación 1 Completada: Columnas estandarizadas (sin tildes ni espacios).")
print("-" * 50)
# Transformación 2: Tratamiento de Valores Nulos

nums = ['total_docentes', 'total_estudiantes', 'estudiantes_f', 'estudiantes_m']
df[nums] = df[nums].fillna(0).astype(int)
print("Transformación 2 Completada: Valores nulos sustituidos por 0 en columnas cuantitativas.")
print("   * Nota técnica: El valor 0 representa 'sin datos reportados', no ausencia de personas.")
print("-" * 50)


# Transformación 3: Eliminación de Registros Duplicados

registros_antes = df.shape[0]
# Usamos el par solicitado por el documento
df = df.drop_duplicates(subset=['cod_amie', 'anio_lectivo'])
print(f"Transformación 3 Completada: Se eliminaron {registros_antes - df.shape[0]} filas duplicadas.")
print("-" * 50)

# Transformación 4: Validación de Consistencia de Matrícula

inconsistentes = df[df['estudiantes_f'] + df['estudiantes_m'] != df['total_estudiantes']]
print(f"Transformación 4 Completada: Validación de consistencia ejecutada.")
print(f"   * Registros inconsistentes detectados: {len(inconsistentes)}")
print("   * Decisión documentada: Se conservan los registros para mantener la representatividad institucional.")
print("-" * 50)
#Aqui gurdamos el nuevo data set limpio
df.to_csv(archivo_limpio, sep=';', index=False, encoding='latin-1')

print(f" El archivo limpio se guardó en: {archivo_limpio}")
print(f" Registros finales listos para análisis: {df.shape[0]}")
