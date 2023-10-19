import os
import hubspot
from hubspot.crm.contacts import ApiException
import pandas as pd
import pprint
import time

from abc import ABC


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


class csvHandler:

    # # stages_map = {
# #     '164119795': ['qualified_to_buy', 'b2c'],
# #     '174044097': ['ready_to_close', 'b2c'],
# #     '164119796': ['locked', 'b2c'],
# #     '153945067': ['signal_paid', 'b2c'],
# #     '153945068': ['docs_received', 'b2c'],
# #     '153945070': ['deposit_paid', 'b2c'],
# #     '153945071': ['deal_won', 'b2c'],
# #     '153945072': ['deal_lost', 'b2c'],
# #     '192933592': ['Canceled', 'b2c'],
# #     '162366685': ['first_contact', 'awareness'],
# #     '162366688': ['to_be_called', 'awareness'],
# #     '162366686': ['second_contact', 'awareness'],
# #     '163112908': ['second_call', 'awareness'],
# #     '162366687': ['third_contact', 'awareness'],
# #     '162366690': ['send_to_sales', 'awareness'],
# #     '164119259': ['send_to_nurturing', 'awareness'],
# #     '173292239': ['send_to_b2b', 'awareness'],
# #     '162366689': ['remove_contact', 'awareness'],
# # }
    project_code = {
        'Prime Tower' : 'PT',
        'Horizon View' : 'HV',
        'Land' : 'LD'
        }

    stages = {'locked':'164119796',
    'signal_paid':'153945067',
    'deposit_paid':'153945070',
    'deal_won':'153945071'}

    CONTACT_PROPERTIES = [
    'firstname'         ,  # 'nombre clientes'
    'lastname'          ,  # 'nombre clientes'
    'hs_object_id'      ,  # 'id hubspot cliente'
    'sage_code'         ,  # 'codigo sage'
    'sap_code'          ,  # 'codigo sap'
     ]

    DEAL_PROPERTIES = [
    'project_name'                  ,  # 'proyecto'
    'building'                      ,  # 'edificio'
    'apartment_number'              ,  # 'unidad'
    'collaborator'                  ,  # 'id broker'
    'insurance_policy'              ,  # 'poliza de seguro'
    'createdate'                    ,  # 'fecha de inicio'
    'signal_payment_date'           ,  # 'fecha ingreso reserva'
    'reservation_contract_date'     ,  # 'fecha contrato reserva'
    'booking__payment_date'         ,  # 'fecha ingreso inicial'
    'contract_date'                 ,  # 'fecha firma contrato'
    'contract_status'               ,  # 'estado del contrato'
    'delivery_date'                 ,  # 'fecha_entrega'
    'amount'                        ,  # 'precio de venta'
    'signal_value'                  ,  # 'importe teorico reserva'
    'reservation_real_paid_value'   ,  # 'importe real reserva'
    'initial_paid_value'            ,  # 'importe teorico inicial'      
    'actual_amount_of_the_deposit'  ,  # 'importe real inicial'
    'bank_account'                  ,  # 'cuenta de destino'
    'fee_number'                    ,  # 'num cuotas'
    'recurring_fee_period'          ,  # 'periodo de las cuotas'
    'total_fee_value'               ,  # 'total de las cuotas'
    'dealstage'                     ,  # 'Estado del producto' (locked, unpaid....)

    # 'building_level'                ,
    # 'dealstage'                     , 
    # 'hubspot_owner_id'              
    ]

    def __init__(self):
        self.client = ClerhpHubSpot()
        # Tu token de acceso de HubSpot
        self.csv_path = r'C:\Users\Pablo Gris\Documents\0_CLERHP\0_Marketing\bbdd'
        self.df_deals = None
        self.df_customer_by_deal = None
        self.amount = None
        self.theoretical_initial_paid = None
        self.total_not_fulled_code_project = 0.0
        self.total_fee_value = None
        # self.total_empty_customers_named= 0.0
        # self.total_empty_customers_ids= 0.0
        self.csv_properties={
            'codigo' : []                   , # deal calculado :project_code[project_name] + 'building' +"-AP" + 'apartment_number'
            'proyecto' : []                 , # deal:'project_name'
            'edificio' : []                 , # deal:'building'
            'unidad' : []                   , # deal:'apartment_number'
            'nombre clientes': []           , # client calculado: 'firstname' + 'lastname'
            'id broker':[]                  , # deal:  collaborator
            #'broker':[]                     , # deal calculado: 'mapeo con collaborator'
            'poliza de seguro':[]           , # deal: 'insurance_policy'
            'fecha de inicio':[]            , # deal: 'createdate'
            'fecha ingreso reserva':[]      , # deal: 'signal_payment_date'
            'fecha contrato reserva':[]     , # deal: 'reservation_contract_date'
            'fecha ingreso inicial':[]      , # deal: 'booking__payment_date'
            'fecha firma contrato':[]       , # deal:'contract_date'
            'estado del contrato':[]        , # deal: 'contract_status'
            'fecha entrega':[]              , # deal: 'delivery_date'
            'precio de venta':[]            , # deal: 'amount'
            'importe teorico reserva':[]    , # deal: 'signal_value'
            'importe real reserva': []      , # deal: 'reservation_real_paid_value'
            'diferencia reserva':[]         , # deal calculado: 'importe teorico reserva' - 'importe real reserva'
            'importe teorico inicial':[]    , # deal: 'initial_paid_value'
            'importe real inicial':[]       , # deal: 'actual_amount_of_the_deposit'
            'diferencia inicial': []        , # deal calculado:'importe teorico inicial' - 'importe real inicial'
            'cuenta de destino':[]          , # deal: 'bank_account'
            'num cuotas':[]                 , # deal: 'fee_number'
            'periodo de las cuotas':[]      , # deal:'recurring_fee_period'
            'total de las cuotas':[]        , # deal:'total_fee_value'
            'porcentaje inicial':[]         , # calculado sobre amount : round((valor_inicial/precio_final)*100, 2)
            'porcentaje cuotas':[]          , # 'calculado total_fee_value sobre amount (precio final)':  round(('total de las cuotas'/self.precio_final)*100, 2)
            'porcentaje a la entrega':[]    , # 'calculado sobre amount (precio final)', # TODO: Pendiente
            'id hubspot cliente':[]         , # client:'hs_object_id',
            'codigo sage':[]                , # client:'sage_code',
            'codigo sap':[]                 , # client'sap_code'
            }

    def set_df_customer_by_deal(self, row):
        self.df_customer_by_deal = self.client.get_contact_data_from_id_deal(row['id'], csvHandler.CONTACT_PROPERTIES)

        if self.df_customer_by_deal is None:
            print("")
            print("-------------------------------------------------------------------------------------------")
            print(f"ERROR -  deal {row['id']}: Dataframe for contacts cannot generated!!!")
            print(f"All client data associated with deal {row['id']} cannot be filled, so will be ignored.")
            print("-------------------------------------------------------------------------------------------")


    def set_df_deals(self, b_locked):

        df =  self.client.get_deals(csvHandler.DEAL_PROPERTIES)

        if b_locked:
            self.df_deals =  df.loc[
                                (df['properties.dealstage'] ==  csvHandler.stages['locked']) |  
                                (df['properties.dealstage'] ==  csvHandler.stages['signal_paid']) |  
                                (df['properties.dealstage'] ==  csvHandler.stages['deposit_paid'])|
                                (df['properties.dealstage'] ==  csvHandler.stages['deal_won'])
                                ]
        else:

            self.df_deals =  df.loc[
                                (df['properties.dealstage'] ==  csvHandler.stages['signal_paid']) |  
                                (df['properties.dealstage'] ==  csvHandler.stages['deposit_paid'])|
                                (df['properties.dealstage'] ==  csvHandler.stages['deal_won'])
                                ]

        if self.df_deals is None:
            raise ValueError("Dataframe for deals cannot generated!!!")


    def set_csv_file(self,deals_filename):

        debug = False
        if debug:
            print(f"codigo :{len(self.csv_properties['codigo'])}")
            print(f"proyecto :{len(self.csv_properties['proyecto'])}")
            print(f"edificio :{len(self.csv_properties['edificio'])}")
            print(f"nombre clientes :{len(self.csv_properties['nombre clientes'])}")
            print(f"id broker :{len(self.csv_properties['id broker'])}")
            print(f"poliza de seguro :{len(self.csv_properties['poliza de seguro'])}")
            print(f"fecha de inicio :{len(self.csv_properties['fecha de inicio'])}")
            print(f"fecha ingreso reserva :{len(self.csv_properties['fecha ingreso reserva'])}")
            print(f"fecha contrato reserva :{len(self.csv_properties['fecha contrato reserva'])}")
            print(f"fecha ingreso inicial :{len(self.csv_properties['fecha ingreso inicial'])}")
            print(f"fecha firma contrato :{len(self.csv_properties['fecha firma contrato'])}")
            print(f"estado del contrato :{len(self.csv_properties['estado del contrato'])}")
            print(f"fecha entrega :{len(self.csv_properties['fecha entrega'])}")
            print(f"precio de venta :{len(self.csv_properties['precio de venta'])}")
            print(f"importe teorico reserva :{len(self.csv_properties['importe teorico reserva'])}")
            print(f"importe real reserva :{len(self.csv_properties['importe real reserva'])}")
            print(f"diferencia reserva :{len(self.csv_properties['diferencia reserva'])}")
            print(f"importe teorico inicial :{len(self.csv_properties['importe teorico inicial'])}")
            print(f"importe real inicial :{len(self.csv_properties['importe real inicial'])}")
            print(f"diferencia inicial :{len(self.csv_properties['diferencia inicial'])}")
            print(f"cuenta de destino :{len(self.csv_properties['cuenta de destino'])}")
            print(f"num cuotas :{len(self.csv_properties['num cuotas'])}")
            print(f"periodo de las cuotas :{len(self.csv_properties['periodo de las cuotas'])}")
            print(f"total de las cuotas :{len(self.csv_properties['total de las cuotas'])}")
            print(f"porcentaje inicial :{len(self.csv_properties['porcentaje inicial'])}")
            print(f"porcentaje cuotas :{len(self.csv_properties['porcentaje cuotas'])}")
            print(f"porcentaje a la entrega :{len(self.csv_properties['porcentaje a la entrega'])}")
            print(f"id hubspot cliente :{len(self.csv_properties['id hubspot cliente'])}")
            print(f"codigo sage :{len(self.csv_properties['codigo sage'])}")
            print(f"codigo sap:{len(self.csv_properties['codigo sap'])}")
        
        df_fields = pd.DataFrame(self.csv_properties)
        df_fields.to_csv(os.path.join(self.csv_path, deals_filename + '.csv'), index=False)


    def set_project_code(self,row):
        if ( isinstance(row["properties.project_name"],str) and len(row["properties.project_name"]) != 0) and \
           ( isinstance(row["properties.building"],str) and len(row["properties.building"]) != 0) and \
           ( isinstance(row["properties.apartment_number"],str) and len(row["properties.apartment_number"]) != 0):
            self.csv_properties['codigo'].append(csvHandler.project_code[row["properties.project_name"]] + row["properties.building"] +"-AP" + row["properties.apartment_number"])
        else:
            if not isinstance(row["properties.project_name"],str) or len(row["properties.project_name"]) == 0:
                print(f"deal {row['id']}: project name is empty!")
            if not isinstance(row["properties.building"],str) or len(row["properties.building"]) == 0:
                print(f"deal {row['id']}: building is empty!")
            if not isinstance(row["properties.apartment_number"],str) or len(row["properties.apartment_number"]) == 0:
                print(f"deal {row['id']}: apartment number is empty!")

            self.total_not_fulled_code_project+=1
            self.csv_properties['codigo'].append("FAIL -  SOME EMPTY DATA")

    def set_customer_code(self, row, code_hub, code_clerhp):
        id = row['id']
        tot_codes = ""
        if self.df_customer_by_deal is not None and self.df_customer_by_deal['contacts'].size != 0:
            for i_contact in range(len(self.df_customer_by_deal['contacts'])):
                code = self.df_customer_by_deal.loc[self.df_customer_by_deal.index[i_contact], code_hub]
                if code is None:
                    print(f"deal {id}: There is contact with non-associated {code_clerhp}")
                    code = "code_UNKNOWN"
                if i_contact == 0:
                    tot_codes = code
                else:
                    tot_codes+=";" + code

            self.csv_properties[code_clerhp].append(tot_codes)
        else:
            print(f"deal {id}: There is not contact with {code_clerhp}!")
            self.csv_properties[code_clerhp].append(f"EMPTY code {code_clerhp} CUSTOMER LIST") 

    def set_customers_ids(self, row):
        id = row['id']
        tot_ids = ""
        if self.df_customer_by_deal is not None and self.df_customer_by_deal['contacts'].size != 0:
            for i_contact in range(len(self.df_customer_by_deal['contacts'])):
                ids = self.df_customer_by_deal.loc[self.df_customer_by_deal.index[i_contact], 'hs_object_id']
                if ids is None:
                    print(f"deal {id}: There is contact with non-associated id")
                    ids = "IDS_UNKNOWN"
                if i_contact == 0:
                    tot_ids = ids
                else:
                    tot_ids+=";" + ids

            self.csv_properties['id hubspot cliente'].append(tot_ids)
        else:
            print(f"deal {id}: There is not contact ids!")
            self.csv_properties['id hubspot cliente'].append("EMPTY IDS CUSTOMER LIST") 

    def set_customers_names(self,row):
        id = row['id']
        tot_names = ""
        if self.df_customer_by_deal is not None and self.df_customer_by_deal['contacts'].size != 0:
            for i_contact in range(len(self.df_customer_by_deal['contacts'])):
                first_name = self.df_customer_by_deal.loc[self.df_customer_by_deal.index[i_contact], 'firstname']
                if first_name is None:
                    print(f"deal {id}: There is contact with non-associated first name")
                    first_name = "FN_UNKNOWN"
                if i_contact == 0:
                    tot_names = first_name
                else:
                    tot_names+=";" + first_name

                last_name = self.df_customer_by_deal.loc[self.df_customer_by_deal.index[i_contact], 'lastname']
                if last_name is None:
                    print(f"deal {id}: There is contact with non-associated first name")
                    last_name = "LN_UNKNOWN"          
                tot_names += " " +last_name

            self.csv_properties['nombre clientes'].append(tot_names)
        else:
            print(f"deal {id}: There is not contact names!")
            self.csv_properties['nombre clientes'].append("EMPTY NAME CUSTOMER LIST") 

    def set_amount(self, row):
        if isinstance(row["properties.amount"], str) and len(row["properties.amount"])!= 0:
            self.amount = float(row["properties.amount"])
            self.csv_properties['precio de venta'].append(row["properties.amount"])
            if self.amount<= 0.0:
                print("TOTAL AMOUNT LESS or EQUAL TO ZERO!!! Please, review entry data.")
        else:
            self.csv_properties['precio de venta'].append(None)
            print(f"deal {row['id']}: total amount is not filled!!")

    def set_booking(self,row):

        theoretical_booking =  None
        if row["properties.signal_value"] != None and len(row["properties.signal_value"]) != 0:
            theoretical_booking = float(row["properties.signal_value"])
            self.csv_properties['importe teorico reserva'].append(row["properties.signal_value"])
        else:        
            print(f"deal {row['id']}: theoretical signal value is not filled!!")
            self.csv_properties['importe teorico reserva'].append(theoretical_booking)
        
        real_booking = None
        if row["properties.reservation_real_paid_value"] != None and len(row["properties.reservation_real_paid_value"]) != 0:
            real_booking =float(row["properties.reservation_real_paid_value"])
            self.csv_properties['importe real reserva'].append(row["properties.reservation_real_paid_value"]) 
        else:
            print(f"deal {row['id']}: real signal value is not filled!!")
            self.csv_properties['importe real reserva'].append(real_booking)
        

        if theoretical_booking and real_booking:
            self.csv_properties['diferencia reserva'].append(str(theoretical_booking - real_booking))
        else:
            self.csv_properties['diferencia reserva'].append(None)
            

    def set_initial_paid(self, row):

        if row["properties.initial_paid_value"] != None and len(row["properties.initial_paid_value"]) != 0:
            self.theoretical_initial_paid = float(row["properties.initial_paid_value"])
            self.csv_properties['importe teorico inicial'].append(row["properties.initial_paid_value"])      
        else:      
            self.csv_properties['importe teorico inicial'].append(self.theoretical_initial_paid)    
            print(f"deal {row['id']}: initial paid value is not filled!!")
        

        real_initial_paid = None
        if row["properties.actual_amount_of_the_deposit"] != None and len(row["properties.actual_amount_of_the_deposit"]) != 0:
            real_initial_paid =float(row["properties.actual_amount_of_the_deposit"])
            self.csv_properties['importe real inicial'].append(row["properties.actual_amount_of_the_deposit"])
        else:
            self.csv_properties['importe real inicial'].append(real_initial_paid)
            print(f"deal {row['id']}: real initial paid is not filled!!")
        

        if self.theoretical_initial_paid and real_initial_paid:
            self.csv_properties['diferencia inicial'].append(str(self.theoretical_initial_paid - real_initial_paid))
        else:
            self.csv_properties['diferencia inicial'].append(None)

    def set_fee_data(self, row):

        if row["properties.total_fee_value"] != None and len(row["properties.total_fee_value"]) != 0:
            self.total_fee_value = float(row["properties.total_fee_value"]) 
            self.csv_properties['total de las cuotas'].append(row["properties.total_fee_value"])     
            self.csv_properties['periodo de las cuotas'].append(row["properties.recurring_fee_period"])
            self.csv_properties['num cuotas'].append(row["properties.fee_number"])
        else:        
            print(f"deal {row['id']}: total fee value is not filled!!. Recurring fee period and fee number will be ignored")
            self.csv_properties['total de las cuotas'].append(None)         
            self.csv_properties['periodo de las cuotas'].append(None)
            self.csv_properties['num cuotas'].append(None)
        


    def set_initial_percentage(self, row):
        initial_percent = None
        if self.theoretical_initial_paid and self.amount:
                initial_percent = round((self.theoretical_initial_paid/self.amount)*100,2)
        else:
            print(f"deal {row['id']}: theoretical initial paid: {self.theoretical_initial_paid}; total amount: {self.amount} -> initial percentage is None")

        self.csv_properties['porcentaje inicial'].append(str(initial_percent))

    def set_fee_percentage(self,row):
        fee_percent = None
        if self.total_fee_value and self.amount:
            fee_percent = round((self.total_fee_value/self.amount)*100,2)
        else:
            print(f"deal {row['id']}: total fee value: {self.total_fee_value}; total amount: {self.amount} -> fee percentage is None")

        self.csv_properties['porcentaje cuotas'].append(str(fee_percent))

    def set_delivery_percentage(self, row):

        if self.total_fee_value and self.theoretical_initial_paid and self.amount:
            csv_handler.csv_properties['porcentaje a la entrega'].append(str(round(((self.amount - (self.total_fee_value + self.theoretical_initial_paid ))/self.amount) *100, 2)))
        else:
            csv_handler.csv_properties['porcentaje a la entrega'].append(None)
            print(f"deal {row['id']}: total fee value: {self.total_fee_value}; theoretical initial paid: {self.theoretical_initial_paid}; total amount: {self.amount} -> delivery percentage is None")

     

csv_handler = csvHandler()
csv_handler.set_df_deals(True)

for index, row in csv_handler.df_deals.iterrows():
    csv_handler.set_df_customer_by_deal(row)
    csv_handler.set_project_code(row)
    csv_handler.csv_properties['proyecto'].append(row["properties.project_name"])
    csv_handler.csv_properties['edificio'].append(row["properties.building"])
    csv_handler.csv_properties['unidad'].append(row["properties.apartment_number"])
    csv_handler.set_customers_names(row)
    csv_handler.csv_properties['id broker'].append(row["properties.collaborator"])
    # csv_handler.csv_properties['broker'].append(row["properties.collaborator"]) PENDIENTE
    csv_handler.csv_properties['poliza de seguro'].append(row["properties.insurance_policy"])
    csv_handler.csv_properties['fecha de inicio'].append(row["properties.createdate"])
    csv_handler.csv_properties['fecha ingreso reserva'].append(row["properties.signal_payment_date"])
    csv_handler.csv_properties['fecha contrato reserva'].append(row["properties.reservation_contract_date"])
    csv_handler.csv_properties['fecha ingreso inicial'].append(row["properties.booking__payment_date"])
    csv_handler.csv_properties['fecha firma contrato'].append(row["properties.contract_date"])
    csv_handler.csv_properties['estado del contrato'].append(row["properties.contract_status"])
    csv_handler.csv_properties['fecha entrega'].append(row["properties.delivery_date"])
    csv_handler.set_amount(row)
    csv_handler.set_booking(row)
    csv_handler.set_initial_paid(row)
    csv_handler.csv_properties['cuenta de destino'].append(row["properties.bank_account"])
    csv_handler.set_fee_data(row)
    csv_handler.set_initial_percentage(row)
    csv_handler.set_fee_percentage(row)
    csv_handler.set_delivery_percentage(row)
    csv_handler.set_customers_ids(row)
    csv_handler.set_customer_code(row, 'sage_code', 'codigo sage')
    csv_handler.set_customer_code(row, 'sap_code', 'codigo sap')


print(f"TOTAL NUMBER OF DEALS WITH the project name, building or apartment number not filled properly:{int(csv_handler.total_not_fulled_code_project)}")

csv_handler.set_csv_file("obtained_data")



#     for index, row in df_deals.iterrows():
#         if ( isinstance(row["properties.project_name"],str) and len(row["properties.project_name"]) != 0) and \
#            ( isinstance(row["properties.building"],str) and len(row["properties.building"]) != 0) and \
#            ( isinstance(row["properties.apartment_number"],str) and len(row["properties.apartment_number"]) != 0):
#             FIELDS['codigo'].append(PROJECT_CODE[row["properties.project_name"]] + row["properties.building"] +"-AP" + row["properties.apartment_number"])
#         else:
#             FIELDS['codigo'].append("FAIL -  SOME EMPTY DATA")
#         FIELDS['proyecto'].append(row["properties.project_name"])
#         FIELDS['edificio'].append(row["properties.building"])
#         FIELDS['unidad'].append(row["properties.apartment_number"])
#         FIELDS['clientes'].append(get_customers_full_names(row["id"]))
#         FIELDS['poliza de seguro'].append(row["properties.insurance_policy"])
#         FIELDS['estado del contrato'].append(row["properties.contract_status"])
#         FIELDS['fecha firma contrato'].append(row["properties.contract_date"])
#         FIELDS['broker'].append(row["properties.collaborator"])
#         FIELDS['fecha reserva'].append(row["properties.reservation_contract_date"])
#         FIELDS['cuenta destino'].append(row["properties.bank_account"])
#         # FIELDS['codigo sage'].append(row["properties.sage_code"])
#         # FIELDS['codigo sap'].append(row["properties.sap_code"])


# iterate through each row and select
# 'Name' and 'Stream' column respectively.
# for i in df.index:
#     print(df['Name'][ind], df['Stream'][ind])


# for key, value in df_deals.iteritems():
#         print(f"key:{key}")
#         print(f"value:{value}")







# if __name__ == "__main__":

#     def normalize(s):
#         replacements = (
#             ("á", "a"),
#             ("é", "e"),
#             ("í", "i"),
#             ("ó", "o"),
#             ("ú", "u"),
#         )
#         for a, b in replacements:
#             s = s.replace(a, b).replace(a.upper(), b.upper())
#         return s

#     def get_customers_full_names(id):
#         tot_names = ""
#         df_contacts = client.get_contact_data_from_id_deal(id, CONTACT_PROPERTIES)
#         if df_contacts is not None:
#             if df_contacts['contacts'].size != 0:
#                 for i_contact in range(len(df_contacts['contacts'])):
#                     first_name = df_contacts.loc[df_contacts.index[i_contact], 'firstname']
#                     if first_name is None:
#                         first_name = "FN_UNKNOWN"
#                     if i_contact == 0:
#                         tot_names=first_name
#                     else:
#                         tot_names+=";" + first_name

#                     last_name = df_contacts.loc[df_contacts.index[i_contact], 'lastname']
#                     if last_name is None:
#                         last_name = "LN_UNKNOWN"
                    
#                     tot_names += " " +last_name

#                 return tot_names
#             else:
#                 return "EMPTY CUSTOMER LIST"
#         else:
#             return "EMPTY CUSTOMER LIST"

#     start_time = time.time()

#     client = ClerhpHubSpot()


#     # download all deals
#     df_deals = client.get_deals(DEAL_PROPERTIES)
#     debug_remove_by_deal_Stage= False

#     if debug_remove_by_deal_Stage:
#         print(df_deals.shape)
#         print(df_deals['properties.dealstage'])

    
#     # remove deals with non properly dealstage:
#     # TODO: Mejorar con el map
#      #     '164119796': ['locked', 'b2c'], 
#     # '153945067': ['signal_paid', 'b2c'],  
#     # '153945070': ['deposit_paid', 'b2c'], 
#     # '153945071': ['deal_won', 'b2c']
#     df_deals = df_deals.loc[(df_deals['properties.dealstage'] ==  '164119796') |  
#                             (df_deals['properties.dealstage'] ==  '153945067') |
#                             (df_deals['properties.dealstage'] ==  '153945070') |
#                             (df_deals['properties.dealstage'] ==  '153945071')
#                             ]

#     if debug_remove_by_deal_Stage:
#         print(df_deals.shape)
#         print(df_deals['properties.dealstage'])

#         # Escribir el DataFrame a un archivo CSV
#         deals_filename = 'deals_2_export.csv'
#         df_deals.to_csv(os.path.join(csv_path, deals_filename), index=False)


#     for index, row in df_deals.iterrows():
#         if ( isinstance(row["properties.project_name"],str) and len(row["properties.project_name"]) != 0) and \
#            ( isinstance(row["properties.building"],str) and len(row["properties.building"]) != 0) and \
#            ( isinstance(row["properties.apartment_number"],str) and len(row["properties.apartment_number"]) != 0):
#             FIELDS['codigo'].append(PROJECT_CODE[row["properties.project_name"]] + row["properties.building"] +"-AP" + row["properties.apartment_number"])
#         else:
#             FIELDS['codigo'].append("FAIL -  SOME EMPTY DATA")
#         FIELDS['proyecto'].append(row["properties.project_name"])
#         FIELDS['edificio'].append(row["properties.building"])
#         FIELDS['unidad'].append(row["properties.apartment_number"])
#         FIELDS['clientes'].append(get_customers_full_names(row["id"]))
#         FIELDS['poliza de seguro'].append(row["properties.insurance_policy"])
#         FIELDS['estado del contrato'].append(row["properties.contract_status"])
#         FIELDS['fecha firma contrato'].append(row["properties.contract_date"])
#         FIELDS['broker'].append(row["properties.collaborator"])
#         FIELDS['fecha reserva'].append(row["properties.reservation_contract_date"])
#         FIELDS['cuenta destino'].append(row["properties.bank_account"])
#         # FIELDS['codigo sage'].append(row["properties.sage_code"])
#         # FIELDS['codigo sap'].append(row["properties.sap_code"])


#     print(f"proyecto :{len(FIELDS['proyecto'])}")
#     print(f"edificio :{len(FIELDS['edificio'])}")
#     print(f"unidad :{len(FIELDS['unidad'])}")
#     print(f"clientes :{len(FIELDS['clientes'])}")
#     print(f"poliza de seguro:{len(FIELDS['poliza de seguro'])}")
#     print(f"estado del contrato :{len(FIELDS['estado del contrato'])}")
#     print(f"fecha firma contrato :{len(FIELDS['fecha firma contrato'])}")
#     print(f"broker :{len(FIELDS['broker'])}")
#     print(f"fecha reserva :{len(FIELDS['fecha reserva'])}")
#     print(f"cuenta destino :{len(FIELDS['cuenta destino'])}")


#     df_fields = pd.DataFrame(FIELDS)
#     deals_filename = 'deals_3_export.csv'
#     df_fields.to_csv(os.path.join(csv_path, deals_filename), index=False)







