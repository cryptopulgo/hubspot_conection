import os
import hubspot
from hubspot.crm.contacts import ApiException
import pandas as pd

class HubSpotClient:
    def __init__(self):
        self.token_key = 'pat-eu1-38df2c98-ce41-4316-85ea-f8c709c6b849'
        self.api_client = hubspot.Client.create(access_token=self.token_key)
        self.df_contacts = pd.DataFrame()
        self.df_deals = pd.DataFrame()
        self.limit = 100

    def get_contacts(self, contact_properties):
        after = None
        contacs_api = self.api_client.crm.contacts
        while True:
            try:
                # Obtener una página de contactos
                api_response = contacs_api.basic_api.get_page(limit=self.limit, properties=contact_properties, archived=False, after=after)

                # Convertir la lista de contactos a un DataFrame de pandas y agregarlo al DataFrame de contactos
                df = pd.json_normalize(api_response.to_dict()['results'])
                self.df_contacts = pd.concat([self.df_contacts, df])

                # Comprobar si hay más páginas de contactos por obtener
                if api_response.paging is not None and api_response.paging.next is not None:
                    after = api_response.paging.next.after
                else:
                    break
            except ApiException as e:
                print("Exception when calling basic_api->get_page: %s\n" % e)
                break
        return self.df_contacts

    def get_deals(self, deals_properties):
        after = None
        deals_api = self.api_client.crm.deals
        while True:
            try:
                api_response = deals_api.basic_api.get_page(limit=self.limit, properties=deals_properties, associations=["contacts"], archived=False,
                                                 after=after)
                df = pd.json_normalize(api_response.to_dict()['results'])
                self.df_deals = pd.concat([self.df_deals, df])

                if api_response.paging is not None and api_response.paging.next is not None:
                    after = api_response.paging.next.after
                else:
                    break
            except ApiException as e:
                print("Exception when calling basic_api->get_all: %s\n" % e)
                break
        return self.df_deals


if __name__ == "__main__":
    PROPERTIES = ["lifecyclestage", 'hubspot_owner_id']
    client = HubSpotClient(PROPERTIES)
    df_contacts = client.get_contacts()
    print(df_contacts)
