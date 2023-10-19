import pandas as pd
import os

xls_path = r'C:\Users\Pablo Gris\Documents\0_CLERHP\0_Marketing\bbdd'
# Cargar el archivo excel
df_original = pd.read_excel(os.path.join(xls_path, 'product_data_erp.xlsx'))

id_caracteristica = {
    'Orientation': 'C0018',
    'Views': 'C0019',
    'Tipo':'C000'
}

delete_dic = {
    'ProjectName',
    'Building',
    'Product',
}

change_prop = {
    'Este':'C0020',
    'Nordeste':'C0021',
    'Noroeste':'C0022',
    'Norte':'C0023',
    'N-S':'C0024',
    'Oeste':'C0025',
    'Sureste':'C0026',
    'Suroeste':'C0027',
    'W-E':'C0028',
    'A': 'C0011',
    'B': 'C0012',
    'C': 'C0013',
    'D': 'C0014',
    'E': 'C0015',
    'Boulevard':'C0030',
    'Boulevard-Piscina':'C0031',
    'Farallón':'C0032',
    'Farallón-Boulevard':'C0033',
    'Farallón-Boulevard-Piscina':'C0034',
    'Farallón-piscina':'C0035',
    'Hotel':'C0036',
    'Hotel-Boulevard':'C0037',
    'Hotel-Piscina':'C0038',
    'Piscina':'C0039',
}

# tipo_dict ={
#     'A':'C0011',
#     'B':'C0012',
#     'C':'C0013',
#     'D':'C0014',
#     'E':'C0015',
# }

measure_id = {
    'Bedrooms': 'M012',
    'Baths':'M013',
    'TotalSurface':'M020',
    'InteriorSurface':'M022',
    'BalconySurface':'M024',
    'TotalSurface sqft':'M021',
    'InteriorSurface sqft':'M023',
    'BalconySurface sqft':'M025',
}

# print(df_original.columns)
column_keys = list(measure_id.keys()) + list(id_caracteristica.keys())

# df_orientation_view = df_original[df_original['propiedad'].isin(['Orientation', 'View'])]
# print(df_orientation_view)


def reshape_data(df, column_dict, value_dict=None):
    # Crear una lista vacía para almacenar los DataFrames
    df_list = []
    id_counter = 1

    # Renombrar las columnas del DataFrame antes del bucle for
    df = df.rename(columns=column_dict)

    # Iterar sobre las filas en el DataFrame
    for index, row in df.iterrows():
        # Solo toma las columnas que están en column_keys y también existen en el DataFrame
        column_keys_existing = [col for col in column_dict.values() if col in df.columns]
        row = row[column_keys_existing]

        row_df = pd.DataFrame(row)
        row_df = row_df.reset_index()  # Resetea el índice y agrega una nueva columna
        row_df = row_df.rename(columns={"index": "propiedad", row_df.columns[-1]: "valor"})  # Renombra las columnas

        # Si se proporciona un diccionario de valores, sustituye los valores en la columna "valor" según este diccionario
        if value_dict is not None:
            row_df['valor'] = row_df['valor'].map(value_dict).fillna(row_df['valor'])

        unique_id = f"IDOIPR{id_counter}"
        row_df["id"] = unique_id  # Añade la columna id con el valor de unique_id

        # Hacer 'id' la primera columna
        row_df = row_df[['id', 'propiedad', 'valor']]

        df_list.append(row_df)

        id_counter += 1

    # Concatenar todos los DataFrames en la lista en un solo DataFrame
    df_final = pd.concat(df_list, ignore_index=True)

    return df_final


# Obtiene las columnas que deben estar en cada dataframe
columns_id_car = list(id_caracteristica.keys())
columns_id_car.append('ProjectName')  # Agrega 'Project Name' a la lista de columnas id_car

# Crea los dos dataframes
df_id_car = df_original[columns_id_car]
df_measure_id = df_original.drop(columns_id_car, axis=1)

# Ahora puedes llamar a la función reshape_data en cada dataframe
df_final_id_car = reshape_data(df_id_car, id_caracteristica,change_prop)
df_final_measure_id = reshape_data(df_measure_id, measure_id)


# Reshape
# df_final = reshape_data(df_original)
df_final_id_car.to_csv(os.path.join(xls_path, 'erp_car.csv'), index=False, encoding="utf-8-sig")
df_final_measure_id.to_csv(os.path.join(xls_path, 'erp_mes.csv'), index=False, encoding="utf-8-sig")


# print(df_final)