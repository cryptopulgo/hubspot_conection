import os
from hubspot_api import HubSpotClient
import time

start_time = time.time()
# Tu token de acceso de HubSpot
csv_path = r'C:\Users\Pablo Gris\Documents\0_CLERHP\0_Marketing\bbdd'
contacts_filename = 'contactos_export.csv'
deals_filename = 'deals_export.csv'
contacts_properties = [
    'firstname',
    'lastname',
    'email',
    "lifecyclestage",
    'hs_lifecyclestage_lead_date',
    'hs_lifecyclestage_marketingqualifiedlead_date',
    'hs_lifecyclestage_salesqualifiedlead_date',
    'hs_lifecyclestage_opportunity_date',
    'grade',
    'engagement_scoring',
    'buyer_score',
    'hubspotscore',
    'quality_of_contact_scoring',
    'quality_of_contact',
    'ad_id',
    'lead_campaign_source',
    'hubspot_owner_id',
    'hs_time_to_move_from_lead_to_customer',
    'hs_time_to_move_from_marketingqualifiedlead_to_customer',
    'hs_time_to_move_from_salesqualifiedlead_to_customer',
    'hs_analytics_source_data_1',
    'hs_analytics_source_data_2',
    'hs_analytics_source',
    'utm_campaign',
    'utm_content',
    'utm_medium',
    'utm_source',
    'utm_term',
    'country_of_residence'
    'phone',
    'createdate',
    'hs_email_replied',
    'age',
    'first_conversion_event_name',
]

deals_properties = [
    'amount',
    'project_name',
    'apartment_number',
    'answer_to_first_call',
    'building',
    'building_level',
    'balcony_surface',
    'base_price',
    'bath_number',
    'closedate',
    'collaborator',
    'condo_fee',
    'date_contract_sent_to_client',
    'createdate',
    'bank_account',
    'customer_bank_name',
    'date_2call_awareness',
    'date_2contact_awareness',
    'date_3contact_awareness',
    'date_doscreceived_b2c',
    'date_depositpaid_b2c',
    'date_firstcontact_awareness',
    'date_locked_b2c',
    'date_qualifiedtobuy_b2c',
    'date_readytoclose_b2c',
    'date_removecontact_awareness',
    'date_send2b2b_awareness',
    'date_send2nurturing_awareness',
    'date_send2sales_awareness',
    'date_signalpaid_b2c',
    'date_tobecalled_awareness',
    'date_won_b2c',
    'date_lost_b2c',
    'dealname',
    'hubspot_owner_id',
    'dealstage',
    'delivery_date',
    'final_paid_amount',
    'initial_paid_value',
    'inner_surface',
    'notes_last_updated',
    'notes_last_contacted',
    'lock_date',
    'hs_object_id',
    'fee_number',
    'total_fee_value',
    'recurring_fee_period',
    'rooms_number',
    'signal_value',
    'signal_payment_date',
    'status_of_the_property',
    'status_of_the_sales_contract',
    'total_m2',
    'collaborator',
    'broker_fee',
    'cliente_llamado',
]
client = HubSpotClient()
df_contactos = client.get_contacts(contacts_properties)
df_deals = client.get_deals(deals_properties)

# Escribir el DataFrame a un archivo CSV
df_contactos.to_csv(os.path.join(csv_path, contacts_filename), index=False)
df_deals.to_csv(os.path.join(csv_path, deals_filename), index=False)

#tiempo de ejecucion
elapsed_time =round(time.time() - start_time, 2)
print(f"Tiempo de ejecución: {elapsed_time} segundos")

# 'amount', es el precio de venta (tambien hay que preguntarlo en el pop up)
# 'project_name',
# 'apartment_number',
# 'building',
# 'building_level',
# 'base_price', es el precio que sale en la app de disponibilidad
# 'dealname', es el nombre + apellido (del cliente) + codigo piso (por ejemplo PT4-AP309)
# 'hubspot_owner_id', es el id de hubspot del broker
# 'dealstage', 164119796
# 'lock_date', fecha actual
# 'signal_value', la señal