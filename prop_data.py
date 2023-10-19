import os
import hubspot
from hubspot.crm.contacts import ApiException
import pandas as pd
import pprint
import time


stages_map = {
    '164119796': ['locked', 'b2c'], 
    '153945067': ['signal_paid', 'b2c'],  
    '153945070': ['deposit_paid', 'b2c'], 
    '153945071': ['deal_won', 'b2c']
}

# stages_map = {
#     '164119795': ['qualified_to_buy', 'b2c'],
#     '174044097': ['ready_to_close', 'b2c'],
#     '164119796': ['locked', 'b2c'],
#     '153945067': ['signal_paid', 'b2c'],
#     '153945068': ['docs_received', 'b2c'],
#     '153945070': ['deposit_paid', 'b2c'],
#     '153945071': ['deal_won', 'b2c'],
#     '153945072': ['deal_lost', 'b2c'],
#     '192933592': ['Canceled', 'b2c'],
#     '162366685': ['first_contact', 'awareness'],
#     '162366688': ['to_be_called', 'awareness'],
#     '162366686': ['second_contact', 'awareness'],
#     '163112908': ['second_call', 'awareness'],
#     '162366687': ['third_contact', 'awareness'],
#     '162366690': ['send_to_sales', 'awareness'],
#     '164119259': ['send_to_nurturing', 'awareness'],
#     '173292239': ['send_to_b2b', 'awareness'],
#     '162366689': ['remove_contact', 'awareness'],
# }


CONTACT_PROPERTIES = ['firstname','lastname']
DEAL_PROPERTIES = ['project_name','building','building_level','apartment_number','insurance_policy','contract_status','contract_date',
'collaborator','reservation_contract_date','bank_account','sage_code'
                                                          '','sap_code', 'dealstage', 'hubspot_owner_id']

# Tu token de acceso de HubSpot
csv_path = r'C:\Users\Pablo Gris\Documents\0_CLERHP\0_Marketing\bbdd'


class ClerhpHubSpot:

    token_key = 'pat-eu1-38df2c98-ce41-4316-85ea-f8c709c6b849'
    api_client = hubspot.Client.create(access_token=token_key)
    deals_api = api_client.crm.deals
    contacts_api = api_client.crm.contacts
    limit = 100

    def __init__(self):
        pass


    def get_properties_data_from_id_deal(self, deal_id, property_list, b_dataframe = True):

        try:
            res = ClerhpHubSpot.deals_api.basic_api.get_by_id(deal_id,  properties=property_list, archived=False).to_dict()

            if b_dataframe:
                return pd.DataFrame(data = res)['properties']
            else:
                return res['properties']

        except ApiException as e:
            print("Exception when calling deals_api basic_api->get_by_id: %s\n" % e)


    def get_contact_data_from_id_deal(self, deal_id, property_list, b_dataframe = True):

        api_contacts_by_deal = ClerhpHubSpot.deals_api.basic_api.get_by_id(deal_id, associations=["contacts"])
        if api_contacts_by_deal.associations != None:
            dict_contacts= api_contacts_by_deal.associations["contacts"].results
            property_list.insert(0, "contacts")
            res = dict([(i,[]) for i in property_list])

            try:
                for contact in dict_contacts:
                    api_data_id_contact = ClerhpHubSpot.contacts_api.basic_api.get_by_id(contact.id, properties=property_list)
                    for prop, prop_list in res.items():
                        if prop == "contacts":
                            prop_list.append(contact.id)
                        else:
                            prop_list.append(api_data_id_contact.properties[prop])

                if b_dataframe:
                    return pd.DataFrame(data = res)
                else:
                    return res


            except ApiException as e:
                print("Exception when calling deals_api basic_api->get_by_id: %s\n" % e)
        else:
            print(f"Exception when calling deals_api basic_api->get_by_id: There is not contact association by deal {deal_id}")
            return None


    def get_contacts(self, contact_properties):
        after = None
        df_contacts = pd.DataFrame()
        while True:
            try:
                # Obtener una página de contactos
                api_response = ClerhpHubSpot.contacts_api.basic_api.get_page(limit=ClerhpHubSpot.limit, properties=contact_properties, archived=False, after=after)

                # Convertir la lista de contactos a un DataFrame de pandas y agregarlo al DataFrame de contactos
                df = pd.json_normalize(api_response.to_dict()['results'])
                df_contacts = pd.concat([df_contacts, df])

                # Comprobar si hay más páginas de contactos por obtener
                if api_response.paging is not None and api_response.paging.next is not None:
                    after = api_response.paging.next.after
                else:
                    break
            except ApiException as e:
                print("Exception when calling basic_api->get_page: %s\n" % e)
                break
        return df_contacts

    def get_deals(self, deals_properties):
        after = None
        df_deals = pd.DataFrame()
        while True:
            try:
                api_response = ClerhpHubSpot.deals_api.basic_api.get_page(limit=ClerhpHubSpot.limit, properties=deals_properties, associations=["contacts"], archived=False,
                                                 after=after)

                df = pd.json_normalize(api_response.to_dict()['results'])
                df_deals = pd.concat([df_deals, df])

                if api_response.paging is not None and api_response.paging.next is not None:
                    after = api_response.paging.next.after
                else:
                    break
            except ApiException as e:
                print("Exception when calling basic_api->get_all: %s\n" % e)
                break
        return df_deals


if __name__ == "__main__":

    def get_customers_full_names(id):
        tot_names = ""
        df_contacts = client.get_contact_data_from_id_deal(id, CONTACT_PROPERTIES)
        if df_contacts is not None:
            if df_contacts['contacts'].size != 0:
                for i_contact in range(len(df_contacts['contacts'])):
                    first_name = df_contacts.loc[df_contacts.index[i_contact], 'firstname']
                    if first_name is None:
                        first_name = "FN_UNKNOWN"
                    if i_contact == 0:
                        tot_names=first_name
                    else:
                        tot_names+="," + first_name

                    last_name = df_contacts.loc[df_contacts.index[i_contact], 'lastname']
                    if last_name is None:
                        last_name = "LN_UNKNOWN"
                    
                    tot_names += " " +last_name

                return tot_names
            else:
                return "EMPTY CUSTOMER LIST"
        else:
            return "EMPTY CUSTOMER LIST"

    start_time = time.time()


    fields = {
    'codigo' : [],
    'proyecto' : [],
    'edificio' : [],
    'unidad' : [],
    'clientes' :[],
    'poliza de seguro' : [],
    'estado del contrato' : [],
    'fecha firma contrato' : [],
    'broker' : [],
    'fecha reserva' : [],
    'cuenta destino' : [],
    # 'codigo sage' : [],
    # 'codigo sap' : [],
    }

    project_code = {
        'Prime Tower' : 'PT',
        'Horizon View' : 'HV',
        'Land' : 'LD'
    }

    client = ClerhpHubSpot()


    # download all deals
    df_deals = client.get_deals(DEAL_PROPERTIES)
    debug_remove_by_deal_Stage= False

    if debug_remove_by_deal_Stage:
        print(df_deals.shape)
        print(df_deals['properties.dealstage'])

    
    # remove deals with non properly dealstage:
    # TODO: Mejorar con el map
     #     '164119796': ['locked', 'b2c'], 
    # '153945067': ['signal_paid', 'b2c'],  
    # '153945070': ['deposit_paid', 'b2c'], 
    # '153945071': ['deal_won', 'b2c']
    df_deals = df_deals.loc[(df_deals['properties.dealstage'] ==  '164119796') |  
                            (df_deals['properties.dealstage'] ==  '153945067') |
                            (df_deals['properties.dealstage'] ==  '153945070') |
                            (df_deals['properties.dealstage'] ==  '153945071')
                            ]

    if debug_remove_by_deal_Stage:
        print(df_deals.shape)
        print(df_deals['properties.dealstage'])

        # Escribir el DataFrame a un archivo CSV
        deals_filename = 'deals_2_export.csv'
        df_deals.to_csv(os.path.join(csv_path, deals_filename), index=False)


    for index, row in df_deals.iterrows():
        if ( isinstance(row["properties.project_name"],str) and len(row["properties.project_name"]) != 0) and \
           ( isinstance(row["properties.building"],str) and len(row["properties.building"]) != 0) and \
           ( isinstance(row["properties.apartment_number"],str) and len(row["properties.apartment_number"]) != 0):
            fields['codigo'].append(project_code[row["properties.project_name"]] + row["properties.building"] +"-AP" + row["properties.apartment_number"])
        else:
            fields['codigo'].append("FAIL -  SOME EMPTY DATA")
        fields['proyecto'].append(row["properties.project_name"])
        fields['edificio'].append(row["properties.building"])
        fields['unidad'].append(row["properties.apartment_number"])
        fields['clientes'].append(get_customers_full_names(row["id"]))
        fields['poliza de seguro'].append(row["properties.insurance_policy"])
        fields['estado del contrato'].append(row["properties.contract_status"])
        fields['fecha firma contrato'].append(row["properties.contract_date"])
        fields['broker'].append(row["properties.collaborator"])
        fields['fecha reserva'].append(row["properties.reservation_contract_date"])
        fields['cuenta destino'].append(row["properties.bank_account"])
        # fields['codigo sage'].append(row["properties.sage_code"])
        # fields['codigo sap'].append(row["properties.sap_code"])


    print(f"proyecto :{len(fields['proyecto'])}")
    print(f"edificio :{len(fields['edificio'])}")
    print(f"unidad :{len(fields['unidad'])}")
    print(f"clientes :{len(fields['clientes'])}")
    print(f"poliza de seguro:{len(fields['poliza de seguro'])}")
    print(f"estado del contrato :{len(fields['estado del contrato'])}")
    print(f"fecha firma contrato :{len(fields['fecha firma contrato'])}")
    print(f"broker :{len(fields['broker'])}")
    print(f"fecha reserva :{len(fields['fecha reserva'])}")
    print(f"cuenta destino :{len(fields['cuenta destino'])}")


    df_fields = pd.DataFrame(fields)
    deals_filename = 'deals_3_export.csv'
    df_fields.to_csv(os.path.join(csv_path, deals_filename), index=False)



