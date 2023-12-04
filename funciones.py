def filtrar_por_fechas(dataframe, columna_fecha, fecha_inicio, fecha_fin):
    """
    Filtra un DataFrame por un rango de fechas.
    
    Parámetros:
    dataframe (pd.DataFrame): DataFrame a filtrar.
    columna_fecha (str): Nombre de la columna que contiene las fechas.
    fecha_inicio (str): Fecha de inicio para el filtrado.
    fecha_fin (str): Fecha de fin para el filtrado.
    
    Retorna:
    pd.DataFrame: DataFrame filtrado.
    """
    dataframe[columna_fecha] = pd.to_datetime(dataframe[columna_fecha])
    mask = (dataframe[columna_fecha] >= fecha_inicio) & (dataframe[columna_fecha] <= fecha_fin)
    return dataframe.loc[mask]

def generar_reporte_pivot(dataframe, filas, columnas, valores, funcion_agrupadora):
    """
    Genera un DataFrame pivotado basado en los parámetros dados.
    
    Parámetros:
    dataframe (pd.DataFrame): DataFrame a utilizar.
    filas (list): Columnas del DataFrame a usar como filas en el pivot.
    columnas (list): Columnas del DataFrame a usar como columnas en el pivot.
    valores (list): Columnas cuyos valores se quieren analizar.
    funcion_agrupadora (function): Función de agregación para aplicar a los valores.
    
    Retorna:
    pd.DataFrame: DataFrame pivotado.
    """
    return dataframe.pivot_table(index=filas, columns=columnas, values=valores, aggfunc=funcion_agrupadora, fill_value=0)

def guardar_en_postgresql(dataframe, nombre_tabla, engine, if_exists):
    """
    Guarda un DataFrame en una tabla de PostgreSQL.
    
    Parámetros:
    dataframe (pd.DataFrame): DataFrame a guardar.
    nombre_tabla (str): Nombre de la tabla en la base de datos.
    engine (SQLAlchemy Engine): Motor de conexión a la base de datos.
    if_exists (str): Comportamiento si la tabla existe ('fail', 'replace', 'append').
    
    No retorna nada.
    """
    dataframe.to_sql(nombre_tabla, engine, if_exists=if_exists, index=False)
