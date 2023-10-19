import pandas as pd
import os
import json
from tools import DataFrameProcessor, assign_source
import unidecode
import numpy as np
import numpy as np


# Definir una función para aplicar la sustitución
def replace_suffix(value):
    return value.split('_ES', 1)[0].split('_EN', 1)[0]


def change_df_values(df, column, diccionario):
    new_column = column + '_old'
    df[new_column] = df[column]
    df[column] = df[column].map(diccionario).fillna('')
    return df


def filter_by_date(df, date_column, start_date, end_date):
    """
    Filtra un DataFrame por un rango de fechas.

    Args:
    df (pd.DataFrame): DataFrame a filtrar.
    date_column (str): Nombre de la columna de fecha.
    start_date (str): Fecha de inicio en formato 'YYYY-MM-DD'.
    end_date (str): Fecha de fin en formato 'YYYY-MM-DD'.

    Returns:
    pd.DataFrame: Nuevo DataFrame filtrado.
    """
    # Asegúrate de que la columna de fechas es de tipo datetime
    df[date_column] = pd.to_datetime(df[date_column], format='mixed')

    # Filtra el DataFrame
    return df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]


class DataFilter:
    def __init__(self, df, columns_to_filter):
        self.df = df
        self.columns_to_filter = columns_to_filter

    def filter_by_date(self, date_column, start_date, end_date):
        self.df[date_column] = pd.to_datetime(self.df[date_column], format='mixed')
        self.one_filtered_df = self.df[(self.df[date_column] >= start_date) & (self.df[date_column] <= end_date)]
        return self.one_filtered_df

    def filter_all_columns(self, start_date, end_date):
        self.filtered_df = self.df.copy()
        for column in self.columns_to_filter:
            self.filtered_df[column] = pd.to_datetime(self.filtered_df[column], format='mixed')
            self.filtered_df = self.filtered_df[
                (self.filtered_df[column] >= start_date) & (self.filtered_df[column] <= end_date)]
            # print(self.filtered_df[column])
        self.filtered_df = self.filtered_df.dropna(subset=self.columns_to_filter, how='all')
        return self.filtered_df

    def get_non_null_counts(self):
        """
        Devuelve el número de celdas no nulas en cada columna de la lista para el DataFrame filtrado.

        Returns:
        dict: Un diccionario con los nombres de las columnas como claves y el conteo de celdas no nulas como valores.
        """
        non_null_counts = {}
        for column in self.columns_to_filter:
            non_null_counts[column] = self.filtered_df[column].count()
        return non_null_counts


def parse_contacts(x):
    if x and isinstance(x, str):
        try:
            return [dic['id'] for dic in json.loads(x) if 'id' in dic]
        except json.JSONDecodeError:
            return []
    else:
        return []


# Ruta de los archivos CSV
csv_path = r'C:\Users\Pablo Gris\Documents\0_CLERHP\0_Marketing\bbdd'

# Leer el archivo de datos de campaña, eliminando las ultimas 8 filas
campaign_df = pd.read_csv(os.path.join(csv_path, 'campaign_data.csv')).fillna(0).iloc[:-6]
lead_df = pd.read_csv(os.path.join(csv_path, 'contactos_export.csv')).fillna(0)
deal_df = pd.read_csv(os.path.join(csv_path, 'deals_export.csv')).fillna(0)
user_df = pd.read_csv(os.path.join(csv_path, 'user_data.csv'))
collaborator_df = pd.read_csv(os.path.join(csv_path, 'collaborator_data.csv'), sep=';')

# print(campaign_df.tail())

# campaign_df = campaign_df.rename(columns={'Entity': 'ad_group_name'}).set_index('ad_group_name')
lead_df.columns = lead_df.columns.str.replace('properties.', '', regex=False)
deal_df.columns = deal_df.columns.str.replace('properties.', '', regex=False)
lead_df['source'] = lead_df.apply(lambda row: assign_source(row, 'utm_medium', 'hs_analytics_source'), axis=1)


def remove_rows(df, col_name1, condition1, col_name2, condition2):
    mask = (df[col_name1] != condition1) & (df[col_name2] != condition2)
    df = df[mask]
    return df


def replace_values(df):
    replace_dict = {'177824962': 'Pre_customer', '177790658': 'Withdraw'}
    df['lifecyclestage'] = df['lifecyclestage'].replace(replace_dict)
    return df


def update_quality(df, dealstage_list):
    df.loc[df['lifecyclestage'].isin(dealstage_list), 'quality_of_contact'] = 'Good'
    return df


def concatenate_columns(df):
    df['campaign'] = df.apply(lambda row: str(row['utm_campaign']) + '_' + str(row['utm_content']), axis=1)
    return df


def convert_to_number(value):
    if isinstance(value, str):
        return float(value[:-4])
    return value


def count_values_and_lifecyclestage(df):
    return pd.concat([
        pd.Series({
            'Good': (df['quality_of_contact'] == 'Good').sum(),
            'Fair': (df['quality_of_contact'] == 'Fair').sum(),
            'Bad': (df['quality_of_contact'] == 'Bad').sum(),
            'False prospect': (df['quality_of_contact'] == 'False prospect').sum(),
            'Broker': (df['quality_of_contact'] == 'Broker').sum(),
        }),
        pd.Series({
            'subscriber': (df['lifecyclestage'] == 'subscriber').sum(),
            'lead': (df['lifecyclestage'] == 'lead').sum(),
            'marketingqualifiedlead': (df['lifecyclestage'] == 'marketingqualifiedlead').sum(),
            'salesqualifiedlead': (df['lifecyclestage'] == 'salesqualifiedlead').sum(),
            'opportunity': (df['lifecyclestage'] == 'opportunity').sum(),
            'Pre_customer': (df['lifecyclestage'] == 'Pre_customer').sum(),
            'customer': (df['lifecyclestage'] == 'customer').sum(),
            'evangelist': (df['lifecyclestage'] == 'evangelist').sum(),
            'other': (df['lifecyclestage'] == 'other').sum(),
            'Withdraw': (df['lifecyclestage'] == 'Withdraw').sum()
        })
    ])


quality_map = {
    'opportunity': [],
    'Pre_customer': [],
    'customer': [],
    'evangelist': [],
}

owner_map = {
    490536905: 'pablo gris',
    625969140: 'alberto munoz',
    626062067: 'pascual vilar',
    626071996: 'daniel malky',
    696571343: 'jj alonso',
    859069173: 'esmeralda olivero',
    865948401: 'ana pomares',
    708295361: 'macarena perona',
    865948389: 'inma cerda',
}

to_be_removed = [
    'Network',
    'Status',
    'Tracking Status',
    'Account Name',
    'Campaign',
    'Amount Spent',
    'Cost per deal',
    'Cost per lead',
    'Cost per MQL',
    'Cost per opportunity',
    'Cost per session',
    'Cost per SQL',
    'Customers',
    'Deals',
    'Engagements',
    'Network Conversions',
    'Opportunities',
    'Revenue (Currency Converted)',
    'Sales Qualified Leads',
    'Total Contacts',
    'Revenue',
    'ROI',
    'Marketing Qualified Leads',
]

list_to_keep = [
    'do_bsq_marca_en',
    'do_bsq_marca_es',
    'fl_bsq_marca_en',
    'fl_bsq_marca_es',
    'pr_bsq_marca_en',
    'pr_bsq_marca_es',
    'fl_bsq_generica_es',
    'fl_bsq_generica_en',
    'pr_bsq_generica_es',
    'pr_bsq_generica_en',
    'do_bsq_generica_es',
    'do_bsq_generica_en',
    'do_dsp_remarketing_es',
    'do_dsp_remarketing_en',
    'pr_dsp_remarketing_es',
    'nj_bsq_marca_en',
    'nj_bsq_marca_es',
    'ny_bsq_marca_en',
    'ny_bsq_marca_es',
    'ny_bsq_generica_es',
    'ny_bsq_generica_en',
    'nj_bsq_generica_es',
    'nj_bsq_generica_en',
    'mtl_bsq_generica_es',
    'mtl_bsq_generica_en',
    'to_bsq_generica_es',
    'to_bsq_generica_en',
    'mtl_bsq_marca_en',
    'mtl_bsq_marca_es',
    'to_bsq_marca_en',
    'to_bsq_marca_es',
]

# mapping stages
stages_map = {
    '164119795': ['qualified_to_buy', 'b2c'],
    '174044097': ['ready_to_close', 'b2c'],
    '164119796': ['locked', 'b2c'],
    '153945067': ['reservation_paid', 'b2c'],
    '153945068': ['docs_received', 'b2c'],
    '153945070': ['deposit_paid', 'b2c'],
    '153945071': ['deal_won', 'b2c'],
    '153945072': ['deal_lost', 'b2c'],
    '192933592': ['Canceled', 'b2c'],
    '162366685': ['first_contact', 'awareness'],
    '162366688': ['to_be_called', 'awareness'],
    '162366686': ['second_contact', 'awareness'],
    '163112908': ['second_call', 'awareness'],
    '162366687': ['third_contact', 'awareness'],
    '162366690': ['send_to_sales', 'awareness'],
    '164119259': ['send_to_nurturing', 'awareness'],
    '173292239': ['send_to_b2b', 'awareness'],
    '162366689': ['remove_contact', 'awareness'],
    '258825914': ['in_progress', 'awareness'],
    '257909496': ['visiting_rd', 'awareness'],
}

date_columns = [
    'date_locked_b2c',
    'date_signalpaid_b2c',
    'date_doscreceived_b2c',
    'date_depositpaid_b2c',
    'date_won_b2c',
    'date_lost_b2c',
]

dates_map = {
    'date_tobecalled_awareness': 'to_be_called',
    'date_qualifiedtobuy_b2c': 'qualified_to_buy',
    'date_readytoclose_b2c': 'ready_to_close',
    'date_locked_b2c': 'locked',
    'date_signalpaid_b2c': 'signal_paid',
    'date_doscreceived_b2c': 'docs_received',
    'date_depositpaid_b2c': 'deposit_paid',
    'date_won_b2c': 'deal_won',
    'date_lost_b2c': 'deal_lost',
}

# deal_df['hubspot_owner_id'] = deal_df['hubspot_owner_id'].astype(str)
deal_df['hubspot_owner_id'] = deal_df['hubspot_owner_id'].replace(owner_map)
# print(deal_df['hubspot_owner_id'].tolist())

lead_df = concatenate_columns(lead_df)
lead_df = update_quality(lead_df, quality_map)
lead_df = replace_values(lead_df)
lead_df['campaign'] = lead_df['campaign'].apply(unidecode.unidecode)
# Aplicar la función a la columna 'campaign'
# lead_df['campaign'] = lead_df['campaign'].apply(replace_suffix)
lead_df['campaign'] = lead_df['campaign'].replace(r'(_ES|_EN).*', r'\1', regex=True)
grouped_df = lead_df.groupby('campaign').apply(count_values_and_lifecyclestage).reset_index()
# print(grouped_df)
campaign_df[['Amount Spent (Currency Converted)', 'Cost per contact']] = campaign_df[
    ['Amount Spent', 'Cost per contact']].applymap(convert_to_number)
# print(campaign_df.tail())
# convertir contactos asociados a lista
deal_df['associations.contacts.results'] = deal_df['associations.contacts.results'].str.replace("'", '"')
# deal_df['contact_ids'] = deal_df['associations.contacts.results'].apply(lambda x: [dic['id'] for dic in json.loads(x)])
deal_df['contact_ids'] = deal_df['associations.contacts.results'].apply(parse_contacts)
campaign_df = campaign_df.drop(columns=to_be_removed)
campaign_df = campaign_df.rename(
    columns={'Amount Spent (Currency Converted)': 'amount_spent', 'Leads': 'Contacts', 'Cost per contact': 'PPL',
             'Cost per customer': 'CPA'})
campaign_df['CTR'] = round(campaign_df['Clicks'] / campaign_df['Impressions'] * 100, 2)
campaign_df['CPC'] = round(campaign_df['amount_spent'] / campaign_df['Clicks'], 2)

grouped_df['campaign'] = grouped_df['campaign'].str.lower()
campaign_df['Entity'] = campaign_df['Entity'].str.lower().apply(unidecode.unidecode)

grouped_df.set_index('campaign', inplace=True)
campaign_df.set_index('Entity', inplace=True)

df_joined_data = pd.concat([campaign_df, grouped_df], axis=1).fillna(0)
df_joined_data = df_joined_data[df_joined_data.index.isin(list_to_keep)]
df_joined_data['Contacts'] = df_joined_data[
    ['subscriber', 'lead', 'marketingqualifiedlead', 'salesqualifiedlead', 'opportunity', 'Pre_customer', 'customer',
     'evangelist', 'other', 'Withdraw']].sum(axis=1)
df_joined_data['PPL'] = round(df_joined_data['amount_spent'] / df_joined_data['Contacts'], 2)
df_joined_data['CPA'] = round(df_joined_data['amount_spent'] / (
    df_joined_data[['opportunity', 'Pre_customer', 'customer', 'evangelist']].sum(axis=1)), 2)
df_joined_data['MQL_cost'] = round(df_joined_data['amount_spent'] / (df_joined_data[['marketingqualifiedlead',
                                                                                     'salesqualifiedlead',
                                                                                     'opportunity', 'Pre_customer',
                                                                                     'customer', 'evangelist', 'other',
                                                                                     'Withdraw']].sum(axis=1)), 2)
df_joined_data['SQL_cost'] = round(df_joined_data['amount_spent'] / (
    df_joined_data[['salesqualifiedlead', 'opportunity', 'Pre_customer', 'customer', 'evangelist', 'other']].sum(
        axis=1)), 2)

# Lista de columnas a excluir de la suma
exclude_columns = ['PPL', 'CPA', 'CTR', 'CPC']

# Calcula la suma de las columnas, excluyendo las no deseadas
total_row = df_joined_data.drop(columns=exclude_columns).sum()

# Asigna los resultados a una nueva fila con índice 'total'
df_joined_data.loc['total'] = total_row
df_joined_data.at['total', 'PPL'] = round(
    df_joined_data.at['total', 'amount_spent'] / df_joined_data.at['total', 'Contacts'], 2)
total_sum = df_joined_data.loc['total', ['Pre_customer', 'customer', 'evangelist']].sum()
df_joined_data.at['total', 'CPA'] = round(df_joined_data.at['total', 'amount_spent'] / total_sum, 2)
total_sum = df_joined_data.loc[
    'total', ['marketingqualifiedlead', 'salesqualifiedlead', 'opportunity', 'Pre_customer', 'customer', 'evangelist',
              'other', 'Withdraw']].sum()
df_joined_data.at['total', 'MQL_cost'] = round(df_joined_data.at['total', 'amount_spent'] / total_sum, 2)
total_sum = df_joined_data.loc[
    'total', ['salesqualifiedlead', 'opportunity', 'Pre_customer', 'customer', 'evangelist', 'other']].sum()
df_joined_data.at['total', 'SQL_cost'] = round(df_joined_data.at['total', 'amount_spent'] / total_sum, 2)

df_joined_data['% Good'] = round(df_joined_data['Good'] / df_joined_data['Contacts'] * 100, 2)
df_joined_data['% Fair'] = round(df_joined_data['Fair'] / df_joined_data['Contacts'] * 100, 2)
df_joined_data['% Nurturing'] = round(df_joined_data['Bad'] / df_joined_data['Contacts'] * 100, 2)
df_joined_data['% Fake'] = round(df_joined_data['False prospect'] / df_joined_data['Contacts'] * 100, 2)
df_joined_data['% Broker'] = round(df_joined_data['Broker'] / df_joined_data['Contacts'] * 100, 2)

df_joined_data = df_joined_data.rename(columns={'Bad': 'Nurturing'})
df_joined_data = df_joined_data.fillna(0)
df_joined_data = df_joined_data.replace([np.inf, -np.inf], '')


#agrupamos para crear el informe de estado de los negocios

deal_df['dealstage'] = deal_df['dealstage'].astype(str)
first_values_map = {key: value[0] for key, value in stages_map.items()}
deal_df['stage'] = deal_df['dealstage'].replace(first_values_map)
stages_dic = {k: v[0] for k, v in stages_map.items()}
collaborator_dict = collaborator_df.set_index('id')['Collaborator name'].to_dict()
change_df_values(deal_df, 'dealstage', stages_dic)
change_df_values(deal_df, 'collaborator', collaborator_dict)

stages = deal_df['dealstage'].value_counts().to_dict()

df_last_month = DataFrameProcessor(deal_df, '2023-01-01', '2023-01-01', '2023-06-30')
test = df_last_month.filtered_df_dict['deal_won']
# print(test['df'][['date_won_b2c', 'created_at']],test['count'] )

prepost = df_last_month.df_preprocessed
bloqueos = df_last_month.backfill_based_on_dict(dates_map)
indices = df_last_month.get_indices_based_on_dict(dates_map)
unique = df_last_month.remove_duplicate_indices(indices)
count = df_last_month.get_counts_based_on_dict(unique)





deal_grouped = deal_df.pivot_table(index='hubspot_owner_id', columns='stage', aggfunc='size', fill_value=0)

# Si prefieres resetear el índice para tener 'hubspot_owner_id' como columna en lugar de índice
deal_grouped.reset_index(inplace=True)
deal_grouped['calling'] = deal_grouped['to_be_called'] + deal_grouped['in_progress']
deal_grouped['working_on'] = deal_grouped['in_progress'] + deal_grouped['to_be_called'] + deal_grouped['qualified_to_buy'] + deal_grouped['ready_to_close'] + deal_grouped['visiting_rd']
deal_grouped['admin'] = deal_grouped['locked'] + deal_grouped['reservation_paid'] + deal_grouped['reservation_paid']

filtered_deal_df = deal_df.loc[
    (deal_df['cliente_llamado'] == False) &
    (deal_df['stage'].isin(['to_be_called', 'in_progress']))
]

# Agrupar por 'hubspot_owner_id' y contar el número de filas
pending_call_series = filtered_deal_df.groupby('hubspot_owner_id').size()
# Asignar un nombre a la serie
pending_call_series.name = 'pending_call'
print(pending_call_series)
# Unir la serie con la matriz deal_grouped
deal_grouped = deal_grouped.join(pending_call_series, on='hubspot_owner_id', how='left')

# Renombrar la columna y llenar los NaN con 0 si es necesario
deal_grouped.rename(columns={0: 'pending_call'}, inplace=True)
deal_grouped['pending_call'].fillna(0, inplace=True)

print(deal_grouped)

df_joined_data.to_csv(os.path.join(csv_path, 'filtered_df.csv'), index=True)
deal_grouped.to_csv(os.path.join(csv_path, 'deal_grouped.csv'), index=True)
# print((deal_df.loc[deal_df['id'] == 7140620000])['date_signalpaid_b2c'])
# print(df_joined_data)


