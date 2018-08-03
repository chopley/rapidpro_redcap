import redcap
import pandas as pd
from temba_client.v2 import TembaClient
from temba_client.exceptions import TembaRateExceededError, TembaTokenError, TembaHttpError, TembaNoSuchObjectError
import math
import numpy as np
import json
from requests import post

class RedCap:
    import json
    def read_redcap_credentials_file(self,credential_file,form):
        with open(credential_file, encoding='utf-8') as data_file:
            data = json.loads(data_file.read())
        self.redcap_token = data['redcap_token']
        self.redcap_url = data['redcap_url']
        self.form = form
        
    
    def get_metadata(self):
        payload_metadata = {
            'token': self.redcap_token,
            'content': 'metadata',
            'format': 'json',
            'type': 'eav',
            'rawOrLabel': 'raw',
            'rawOrLabelHeaders': 'raw',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'false',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }

        data_metadata = post(self.redcap_url, data=payload_metadata)
        metadata_df = pd.DataFrame.from_dict(data_metadata.json())
        metadata_df['branch_variable'] = metadata_df['branching_logic'].str.extract('((?<=\[).+?(?=\]))', expand=True)
        metadata_df['branch_variable'] =metadata_df['branch_variable'].str.replace('(','___').str.replace(')','')
        metadata_df['branch_value'] = metadata_df['branching_logic'].str.extract('((?<=\').*?(?=\'))', expand=True)
        #metadata_df = metadata_df[metadata_df['form_name']==self.form]
        self.metadata_df = metadata_df
        return(metadata_df)
    
    def get_records_flat(self):
        payload_records = {
            'token': self.redcap_token,
            'content': 'record',
            'format': 'json',
            'type': 'flat',
            'rawOrLabel': 'Label',
            'rawOrLabelHeaders': 'Label',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'true',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json'
        }

        data_records = post(self.redcap_url, data=payload_records)
        records_df = pd.DataFrame.from_dict(data_records.json())
        self.records_df_flat = pd.DataFrame.from_dict(records_df)
        return(self.records_df_flat)
    
    def get_records(self):
        payload_records = {
            'token': self.redcap_token,
            'content': 'record',
            'format': 'json',
            'type': 'eav',
            'rawOrLabel': 'Label',
            'rawOrLabelHeaders': 'Label',
            'exportCheckboxLabel': 'false',
            'exportSurveyFields': 'true',
            'exportDataAccessGroups': 'false',
            'returnFormat': 'json',
            'forms': self.form
        }

        data_records = post(self.redcap_url, data=payload_records)
        records_df = pd.DataFrame.from_dict(data_records.json())
        n_records = np.int(np.max(records_df['record'])) + 1
        list_records = []
        for i in range(1,n_records):
            rr = records_df[records_df['record']==str(i)].T
            rr = rr.rename(columns = rr.iloc[0]).drop(['record','field_name'])
            r_dict = rr.to_dict('records')[0]
            list_records.append(r_dict)
        self.records_df = pd.DataFrame.from_dict(list_records)
        return(self.records_df)
    
    def get_expected_fields(self):
        expected_fields = []
        for i,r in self.records_df_flat.iterrows():
            expected_fields_temp = []
            for indext,rowt in self.metadata_df.iterrows():
                if (rowt['form_name']) == self.form:
                    if(str(rowt['branch_value']))=='nan':
                        expected_fields_temp.append(rowt['field_name'])
                    if(str(rowt['branch_value']))!='nan':
                        if(str(rowt['branch_value']) == self.records_df_flat[str(rowt['branch_variable'])].iloc[i]):
                            #print(str(rowt['field_name']),str(rowt['branch_variable']), str(rowt['branch_value']), self.records_df_flat[str(rowt['branch_variable'])].iloc[i],redcap.records_df_flat[str(rowt['branch_variable'])].iloc[i])  
                            expected_fields_temp.append(rowt['field_name'])
            expected_fields.append(expected_fields_temp)
        self.expected_fields = expected_fields
        return(expected_fields)

    
    def get_completed_fields(self):
        data_records = self.records_df
        completed_fields_list = []
        for index,row in data_records.iterrows():
            completed = data_records.loc[index][~data_records.loc[index].isna()].index.tolist()
            completed_fields_list.append(completed)
        self.completed_fields_list = completed_fields_list
        return(completed_fields_list)
    
    def get_missing_fields(self):
        expected = self.get_expected_fields()
        completed = self.get_completed_fields()
        difference = []
        for i in range(0,len(completed)):
            difference.append(list(set(expected[i]).difference(completed[i])))
        self.difference = difference
        return(difference)
            


class rapidPro:
    def read_rapidpro_credentials_file(self,credential_file,rapidpro_flow_id):
        with open(credential_file, encoding='utf-8') as data_file:
            data = json.loads(data_file.read())
        self.rapidpro_apikey = data['rapidpro_apikey']
        self.rapidpro_url = data['rapidpro_url']
        self.rapidpro_message_sending_flow = data['rapidpro_message_send_flow']
        self.client = TembaClient(self.rapidpro_url, self.rapidpro_apikey)
    
    def add_contact_to_groups(self,contact_urn, group_names):
        
        #get the groups we need to add them too
        additional_groups = self.client.get_groups(name = group_names)
        #check if contact exists first
        contacts = self.client.get_contacts(urn = contact_urn)
        for contact in contacts.all():
            current_groups_dynamic = contact.groups
            current_groups = self.client.get_groups(contact.groups)
            #this gets a little weird before it gets better
            gg = [] #first establish a blank list to store the uuids of the groups 
            if len(contact.groups) > 0 : #only require this if there were groups associated with the contact before
                for g in current_groups.all(): #here we append all the existing uuids of groups that the contact is associated with
                    gg.append(g.uuid)
            for g in additional_groups.all(): #here we add the uuids of the additional groups that are needed
                gg.append(g.uuid)
            contact = self.client.update_contact(contact_urn,groups = gg)
    
    def add_fields_to_contact(self,contact_urn, field_dict):
        client = TembaClient(self.rapidpro_url, self.rapidpro_apikey)
        #get the groups we need to add them too
        add_fields = client.update_contact(contact_urn, fields = field_dict)
    
    def start_flow(self,contact_urn):
        client = TembaClient(self.rapidpro_url, self.rapidpro_apikey)
        #get the groups we need to add them too
        client.create_flow_start(self.rapidpro_message_sending_flow, urns=[contact_urn])
        
