#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 25 14:45:27 2024

@author: anwar
"""

import functions_framework
import requests
import re
import pandas as pd
from datetime import date, datetime, timedelta
import gspread

#=============================================================================
class FHIR_Base:
#hapi prod
  base_url_kobo     = ""
  bearer_token_kobo = ""
  
#prod
  base_url_kuapp = ""
  bearer_token_kuapp = ""
  
  testing    = True
  debug      = True
  hp_test_no = ""   # nomor pribadi untuk testing

#-----------------------------------------------------------------------------
  def __init__(self):
    self.today        = datetime.today()
    self.today_str    = self.today.strftime("%Y-%m-%d")
    self.server_name  = 'hapi-fhir'
    self.base_url     = self.base_url_kobo
    self.bearer_token = self.bearer_token_kobo
    self.headers = {
        'Authorization': f'Bearer {self.bearer_token}'
    }

#----------------------------------------------------------------------------
  def set_base_url_and_token(self, param='kobo'):
    if param == 'kobo':
      self.server_name  = 'hapi-fhir'
      self.base_url     = self.base_url_kobo
      self.bearer_token = self.bearer_token_kobo
    elif param == 'kuapp':
      self.server_name  = 'fhir-server'      
      self.base_url     = self.base_url_kuapp
      self.bearer_token = self.bearer_token_kuapp

    self.headers = {
        'Authorization': f'Bearer {self.bearer_token}'
    }

#-----------------------------------------------------------------------------
  def get_resource_by_id(self, id, resource_type='Patient'):
    params = {
      '_id': id
    }

    url      = self.base_url + resource_type
    response = requests.get(url, params=params, headers=self.headers)

    response_json = response.json()

    if 'entry' not in response_json:
      return {}

    return response_json['entry'][0]['resource']

#-----------------------------------------------------------------------------
  def get_resource_by_reference(self, reference):
    url = f"{self.base_url}{reference}"
    response = requests.get(url, headers=self.headers)
    if response.status_code == 200:
      response_json = response.json()
      return response_json
    else:
      tmp = dict()
      return tmp
    
#-----------------------------------------------------------------------------
  def get_resource_by_identifier(self, identifier, resource_type):
    params = {
      'identifier': identifier
    }

    url      = self.base_url + resource_type
    response = requests.get(url, params=params, headers=self.headers)
    
    response_json = response.json()

    if 'entry' not in response_json:
      return {}

    return response_json['entry'][0]['resource']

#-----------------------------------------------------------------------------
  def get_resource_id_by_identifier(self, identifier, resource_type="Patient"):
    response = self.get_resource_by_identifier(identifier, resource_type)
    if 'id' in response:
      return response['id']
    else:
      return ''

#-----------------------------------------------------------------------------
  def get_response_by_patient_id(self, patient_id, resource_type):
    url = f"{self.base_url}{resource_type}?subject={patient_id}"
    response = requests.get(url, headers=self.headers)
    if response.status_code == 200:
      response_json = response.json()
      if 'entry' in response_json:
        return response_json['entry']
      else:
        tmp = dict()
        return tmp
    else:
      tmp = dict()
      return tmp
    
#---------------------------------------------------------------------------
  def datetime_to_date(self, idatetime):
    ridatetime = re.search(r'^(\d{4}-\d{2}-\d{2})', idatetime)
    if ridatetime:
      return ridatetime.group(1)
    else:
      return idatetime



#============================================================================
class FHIR_Encounter(FHIR_Base):
  
#-----------------------------------------------------------------------------
  def __init__(self, last_visit_date_str=''):
    FHIR_Base.__init__(self)
    self.__resource_type = "Encounter"

    if last_visit_date_str == '': 
      last_visit_date_str = (self.today - timedelta(days=30)).strftime("%Y-%m-%d")
    
    self.last_visit_date_str = last_visit_date_str
    
    self.params = {
      'date': self.last_visit_date_str
    }

#-----------------------------------------------------------------------------
  def set_params(self, last_visit_date_str=''):
    if last_visit_date_str == '': 
      last_visit_date_str      = (self.today - timedelta(days=30)).strftime("%Y-%m-%d")

    self.last_visit_date_str = last_visit_date_str

    self.params = {
      'date'   : self.last_visit_date_str
    }
      
#-----------------------------------------------------------------------------
  def get_all_response_from_encounter(self, last_visit_date_str=''):
    self.set_params(last_visit_date_str)
            
    url = self.base_url + self.__resource_type
    response = requests.get(url, headers=self.headers, params=self.params)
    
    if response.status_code != 200:
      return {}

    response_json = response.json()
  
    if 'entry' not in response_json:
      return {}
  
    return response_json
        
#----------------------------------------------------------------------------
  def get_organization_reference_from_encounter_resource(self, encounter_resource):
    organization_reference = ''
    if 'serviceProvider' in encounter_resource:
      serviceProvider = encounter_resource['serviceProvider']
      if serviceProvider['type'] == 'Organization':
        organization_reference = serviceProvider['reference']
    
    return organization_reference
  
#----------------------------------------------------------------------------
  def get_encounter_name_from_encounter_resource(self, encounter_resource):
    encounter_name = ''
    
    if 'identifier' in encounter_resource:
      for identifier in encounter_resource['identifier']:
        system = identifier['system']
        rsystem = re.search(r'encounter$', system)
        if rsystem:
          value = identifier['value']
          rvalue = re.search(r'^\d+-(\w+)-\d+$', value)
          if rvalue:
            encounter_name = rvalue.group(1)
      
    return encounter_name

#----------------------------------------------------------------------------
  def encounter_is_anc(self, encounter_name):
    is_anc = False
    rencounter_name = re.search(r'ANC_VISIT', encounter_name.upper())
    if rencounter_name:
      is_anc = True
    
    return is_anc



#============================================================================
class FHIR_Patient(FHIR_Base):
  
#-----------------------------------------------------------------------------
  def __init__(self):
    FHIR_Base.__init__(self)
    self.__resource_type = "Patient"

#----------------------------------------------------------------------------
  def get_patient_mobile_from_patient_resource(self, patient_resource):
    mobile  = ''
    mobile2 = ''
    if 'telecom' in patient_resource:
      for patient_telecom in patient_resource['telecom']:
        if patient_telecom['use'] == "mobile":
          if "value" in patient_telecom:
            mobile = patient_telecom['value']
            rmobile = re.search(r'^\s*(\d+)(\s*[|,; ]\s*(\d+))?\s*$', mobile)
            if rmobile: 
              mobile  = rmobile.group(1)
              mobile2 = rmobile.group(3)
              return mobile, mobile2

    return mobile, mobile2

#----------------------------------------------------------------------------
  def get_patient_city_from_patient_resource(self, patient_resource):
    district = ''
    city     = ''
    if 'address' in patient_resource:
      for address in patient_resource['address']:
        if 'district' in address:
          district = address['district']

        if 'extension' in address:
          for extension in address['extension']:
            if extension['url'] == 'city':
              city = extension['valueString']
    
    return district, city

#----------------------------------------------------------------------------
  def get_patient_name_from_patient_resource(self, patient_resource):
    name = ''
    if 'name' in patient_resource:
      for patient_name in patient_resource['name']:
        if 'text' in patient_name:
          name = patient_name['text']
        else:
          if 'given' in patient_name:
            name = patient_name['given'][0] 
            name += ' '+ patient_name['family']
            
    return name

#----------------------------------------------------------------------------
  def get_mother_identifier_from_patient_resource(self, patient_resource):
    identifier = ''
    if 'identifier' in patient_resource:
      for patient_identifier in patient_resource['identifier']:
        found  = False
        mother = True
      
        if "system" in patient_identifier:
          patient_system = patient_identifier['system'].lower()
          if not found:
            rchild = re.search(r'mother$', patient_system)
            if rchild: 
              mother = True
              found = True
          
          if not found:
            rchild = re.search(r'child$', patient_system)
            if rchild: 
              mother = False
              found  = True
    
          if not found:
            rfather = re.search(r'father$', patient_system)
            if rfather: 
              mother = False
              found  = True
        
        if mother:
          identifier = patient_identifier['value']
          
    return identifier

#----------------------------------------------------------------------------
  def get_extension_patient_pregnancy_status(self, patient_resource):
    pregnancy_status = ''
    if 'extension' in patient_resource:
      for extension in patient_resource['extension']: 
         if 'url' in extension.keys():
           if extension['url'] == 'pregnancy_status':
             pregnancy_status = extension['valueString']

    return pregnancy_status  



#============================================================================
class FHIR_Organization(FHIR_Base):
  
#-----------------------------------------------------------------------------
  def __init__(self):
    FHIR_Base.__init__(self)
    self.__resource_type = "Organization"
    
#----------------------------------------------------------------------------
  def get_puskesmas_from_organization_resource(self, organization_resource):
    puskesmas = ''
    for identifier in organization_resource['identifier']:
      system = identifier['system']
      rsystem = re.search(r'puskesmas$', system)
      if rsystem:
        puskesmas = identifier['value']
        
    return puskesmas

#----------------------------------------------------------------------------
  def get_puskesmas_by_organization_reference(self, organization_reference):
    organization_resource = self.get_resource_by_reference(organization_reference)
    return self.get_puskesmas_from_organization_resource(organization_resource)


#============================================================================
class FHIR_Observation(FHIR_Base):
  
#-----------------------------------------------------------------------------
  def __init__(self):
    FHIR_Base.__init__(self)
    self.__resource_type = "Observation"

#----------------------------------------------------------------------------
  def last_mens_date_to_pregnancy_weeks(self, last_mens_date_str):
    last_mens_date  = datetime.strptime(last_mens_date_str, "%Y-%m-%d")
    result = self.today - last_mens_date
    return round(result.days/7)
  
#-----------------------------------------------------------------------------
  def get_last_mens_date_from_observation_resource(self, observation_resource):
    last_mens_date_str = ''
    if 'code' in observation_resource:
      code = observation_resource['code']
      if 'coding' in code:
        for coding in code['coding']:
          if 'system' in coding:
            system  = coding['system']
            rsystem = re.search(r'loinc.org', system)
            if rsystem:
              if 'code' in coding:
              # Last menstrual period start date
                if coding['code'] == '8665-2':
                  if 'valueDateTime' in observation_resource:
                    last_mens_date_str = observation_resource['valueDateTime']
                    break    

    return last_mens_date_str
  
#-----------------------------------------------------------------------------
  def get_last_mens_date_by_patient_id(self, patient_id):
    last_mens_date_str = ''
    resource           = self.get_response_by_patient_id(patient_id, "Observation")
    for entry in resource:
      observation_resource = entry['resource']
      
      last_mens_date_str = self.get_last_mens_date_from_observation_resource(observation_resource)
    
      if 'component' in observation_resource:
        for component in observation_resource['component']:
          last_mens_date_str = self.get_last_mens_date_from_observation_resource(component)
          if last_mens_date_str: break
          
      if last_mens_date_str: break
                
    return last_mens_date_str

#-----------------------------------------------------------------------------
  def get_last_mens_date_by_identifier(self, identifier):
    patient_id         = self.get_resource_id_by_identifier(identifier)
    last_mens_date_str = self.get_last_mens_date_by_patient_id(patient_id)
    
    return last_mens_date_str

#-----------------------------------------------------------------------------
  def get_last_mens_date_and_usg_weeks_from_observation_resource(self, observation_resource):
    last_mens_date_str = ''
    usg_weeks          = ''
    if 'code' in observation_resource:
      code = observation_resource['code']
      if 'coding' in code:
        for coding in code['coding']:
          if 'system' in coding:
            system  = coding['system']
            rsystem = re.search(r'loinc.org', system)
            if rsystem:
              if 'code' in coding:
              # Last menstrual period start date
                if coding['code'] == '8665-2':
                  if 'valueDateTime' in observation_resource:
                    last_mens_date_str = observation_resource['valueDateTime']
                    break

                # Gestational age based on USG
                if coding['code'] == '11888-5':
                  if 'valueQuantity' in observation_resource:
                    usg_weeks = observation_resource['valueQuantity']['value']
                    break    
              
                # Gestational age based on last menstrual date
                if coding['code'] == '11885-1':
                  if 'valueQuantity' in observation_resource:
                    usg_weeks = observation_resource['valueQuantity']['value']
                    break    

    return last_mens_date_str, usg_weeks
  
#-----------------------------------------------------------------------------
  def get_last_mens_date_and_weeks_by_patient_id(self, patient_id):
    last_mens_date     = ''
    weeks              = 0
    usg_weeks          = 0
    effectiveDateTime  = ''
    resource = self.get_response_by_patient_id(patient_id, "Observation")
    for entry in resource:
      observation_resource = entry['resource']
      
      last_mens_date_str, usg_weeks_str = self.get_last_mens_date_and_usg_weeks_from_observation_resource(observation_resource)
    
      if 'component' in observation_resource:
        for component in observation_resource['component']:
          last_mens_date_str, usg_weeks_str = self.get_last_mens_date_and_usg_weeks_from_observation_resource(component)
          if last_mens_date_str or usg_weeks_str:
            break

      if last_mens_date_str: 
        last_mens_date = last_mens_date_str
        break
      
      if usg_weeks_str: 
        usg_weeks      = usg_weeks_str
               
      if usg_weeks: 
        if 'effectiveDateTime' in observation_resource:
          effectiveDateTime = observation_resource['effectiveDateTime']

    if effectiveDateTime:
      effectiveDate = self.datetime_to_date(effectiveDateTime)
      est_last_mens_date = datetime.strptime(effectiveDate, "%Y-%m-%d") - timedelta(weeks=usg_weeks)
      result = self.today - est_last_mens_date
      weeks = round(result.days/7)
      
    if last_mens_date:
      weeks = self.last_mens_date_to_pregnancy_weeks(last_mens_date)
                  
    return last_mens_date, weeks

#-----------------------------------------------------------------------------
  def get_last_mens_date_and_weeks_by_identifier(self, identifier):
    patient_id = self.get_resource_id_by_identifier(identifier, "Patient")
    return self.get_last_mens_date_and_weeks_by_patient_id(patient_id)


############################################################################
class GoogleSheet:
  gs_report_name = "FHIR WA Report"
  gs_report_id   = ""
  token_filename = ""
  
#----------------------------------------------------------------------------
  def __init__(self, sheet_name, patient_name='mother_name'):
    gc = gspread.service_account(filename=self.token_filename)
    sh = gc.open_by_key(self.gs_report_id)
    
    self.sheet_name = sheet_name

    self.worksheet_list = sh.worksheets()
    self.worksheet      = sh.worksheet(self.sheet_name)
        
    self.sent_patient_name_list = []

    df = pd.DataFrame(self.worksheet.get_all_records(expected_headers=['datetime', 'server_name', 'patient_id', 'identifier', 'no_hp', 'mother_name', 'puskesmas', 'city', 'last_mens_date', 'last_visit_date', 'next_visit_date', 'weeks', 'trimester', 'wa status', 'Date', 'Month', 'WhatsApp']))

    if not df.empty:
      df['mydate'] = df['datetime'].str[:10]
      df = df[df['mydate'] == date.today().strftime("%Y-%m-%d")]
      self.sent_patient_name_list = df[patient_name].tolist()

    print(f'Patient name from google-sheet report: {self.sent_patient_name_list}')
    self.report_list = []
    
#----------------------------------------------------------------------------
  def create_report_list(self, patient_id, identifier, no_hp, mother_name, puskesmas, city, last_mens_date_str, weeks, last_visit_date_str, curr_trimester, status="Not Executed", valid=''):
    now = datetime.now()
    date_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    self.report_list = [date_time_str, self.server_name, patient_id, identifier, no_hp, mother_name, puskesmas, city, last_mens_date_str, last_visit_date_str, self.next_visit_date_str, weeks, curr_trimester, status]
    self.report_list.extend(self.add_data_for_pivot_wa(date_time_str, status))
    if self.testing: self.report_list.append('Testing')

#------------------------------------------------------------------------
  def add_data_for_pivot_wa(self, datetime_str, status):
    adate    = ''
    month    = ''
    whatsapp = ''
    rdatetime = re.search(r'^((\d{4}-\d{2})-\d{2})', datetime_str)
    if rdatetime:
      adate = rdatetime.group(1)
      month = rdatetime.group(2) +'-01'
  
    if status == 200:
      whatsapp = 'Success'
    elif status == 500:
      whatsapp = 'Failed'
    else:
      whatsapp = status

    return [adate, month, whatsapp]


#==========================================================================
class Qontak:
  wa_authorization          = ""
  wa_channel_integration_id = ""

#----------------------------------------------------------------------------
  def __init__(self, message_template_id):
    self.message_template_id = message_template_id
    self.wa_report_log       = []
    
#----------------------------------------------------------------------------
  def collect_wa_log_report(self):
    # Prepare headers with bearer token
    headers = {
        "Authorization": self.wa_authorization
    }
    
    response = requests.get("https://service-chat.qontak.com/api/open/v1/broadcasts/whatsapp?direction=desc", headers=headers)
  
    if response.status_code != 200:
#      raise Exception(f"Error: {response.status_code} - {response.text}")
      return {}
  
    response_json = response.json()
  
    if 'data' not in response_json:
      return {'data': [ ]}
    
    return response_json  
    
#----------------------------------------------------------------------------
  def get_wa_report_log(self, isend_at):
    self.wa_report_log = []
    response = self.collect_wa_log_report()
    print('Total WA log report: '+ str(len(response['data'])))
    for data in response['data']:
      rsend_at = re.search(r'^(\d{4}-\d{2}-\d{2})', data['send_at'])
      if rsend_at:
        if rsend_at.group(1) == isend_at:
          if data['message_template']['id'] == self.message_template_id:
            contact_extra = data['contact_extra']
#            contact_extra['send_at'] = isend_at
            self.wa_report_log.append(contact_extra)
              
    return self.wa_report_log
  
#----------------------------------------------------------------------------
  def check_wa_sent_from_log_by_patient_name(self, parameter, patient_name):
    for wa_sent in self.wa_report_log:
      if parameter in wa_sent:
        if wa_sent[parameter] == patient_name:
            return True
      
    return False


#==========================================================================
class Pro_Reminder(FHIR_Encounter, FHIR_Patient, FHIR_Organization, FHIR_Observation):
  
#-----------------------------------------------------------------------------
  def __init__(self, next_visit_date_str='', days=7):
    self.days_before_wa        = 4
    self.trimester_delta_weeks = (0, 4, 2, 1)
    next_visit_date            = object()
    if next_visit_date_str == '':
      next_visit_date = datetime.today() + timedelta(days = self.days_before_wa)
      next_visit_date_str = next_visit_date.strftime("%Y-%m-%d")
    else:
      next_visit_date = datetime.strptime(next_visit_date_str, "%Y-%m-%d")

    last_visit_date     = next_visit_date - timedelta(days = days)
    last_visit_date_str = last_visit_date.strftime("%Y-%m-%d")
    
    FHIR_Encounter.__init__(self, last_visit_date_str)
    FHIR_Patient.__init__(self)
    FHIR_Organization.__init__(self)
    FHIR_Observation.__init__(self)
    
    self.next_visit_date     = next_visit_date
    self.next_visit_date_str = next_visit_date_str

#----------------------------------------------------------------------------
  def determine_trimester(self, week):
    # Determine the next visit date based on Gestational Age (usia kehamilan)
    ## Minggu 1 - 28 dari kehamilan: 4 minggu
    ## Minggu 28 - 36 dari kehamilan: 2 minggu
    ## Minggu 36 - 40 dari kehamilan: 1 minggu
    week = int(week)
    if week >= 1 and week < 28:
        trimester   = 1
    elif week >= 28 and week < 36:
        trimester   = 2
    elif week >= 36 and week <= 40:
        trimester   = 3
    else:
        trimester   = -1  # Invalid week
  
    return trimester

#----------------------------------------------------------------------------
  def get_condition_PNC_by_patient_id(self, patient_id):
    recordedDate = ''
    found = False
    for response in self.get_response_by_patient_id(patient_id, 'Condition'):
      if 'resource' in response:
        resource = response['resource']
        if 'code' in resource:
          if 'coding' in resource['code']:
            for coding in resource['code']['coding']:
              system = coding['system']
              rsystem = re.search(r'snomed.info', system)
              if rsystem:
                if coding['code'] in ('86569001', '234234234'):
                  found = True
                  break
                
        if found:
          if 'recordedDate' in resource:
            recordedDate = resource['recordedDate']
          else:
            recordedDate = '2024-01-01'
          
          break
    
    return recordedDate
    
#==========================================================================
class WA_Reminder(Pro_Reminder, GoogleSheet, Qontak):
  
#-----------------------------------------------------------------------------
  def __init__(self, next_visit_date_str='', days=7):    
    Pro_Reminder.__init__(self, next_visit_date_str, days)
    GoogleSheet.__init__(self, 'anc_visit_reminder')
    Qontak.__init__(self, 'bfc118d0-fd0f-4a9f-950f-e4952cda3935')
    
    self.curr_patient_identifier_list = []
    
#----------------------------------------------------------------------------
  def get_puskesmas_from_encounter_resource(self, encounter_resource):
    puskesmas = ''
    organization_reference = self.get_organization_reference_from_encounter_resource(encounter_resource)
    if organization_reference:
      puskesmas = self.get_puskesmas_by_organization_reference(organization_reference)

    return puskesmas
    
#----------------------------------------------------------------------------
  def get_and_set_last_visit_date_by_trimester(self, trimester=1):
    last_visit_date = self.next_visit_date - timedelta(weeks=self.trimester_delta_weeks[trimester])
    last_visit_date_str = last_visit_date.strftime("%Y-%m-%d")
    if self.debug:
      print(f'[debug] trimester: {trimester}, last_visit_date: {last_visit_date_str}, next_visit_date_str: {self.next_visit_date_str}')
            
    self.last_visit_date_str = last_visit_date_str
    
    return last_visit_date_str
  
#----------------------------------------------------------------------------
# for example: for trimester 2
# we will send WA reminder for next visit in next 4 days "D"
# search ANC VISIT at Encounter on visit date D - 2 weeks 
# patient on that encounter must have trimester 3 to get WA reminder
  def get_all_mother_by_trimester(self, trimester=1):
    last_visit_date_str = self.get_and_set_last_visit_date_by_trimester(trimester)
    print(self.params)

    result   = []
    response = self.get_all_response_from_encounter()
    if response and 'entry' in response:
      print('Total '+ str(len(response['entry'])) +' visit on '+ self.last_visit_date_str)
      if self.debug:
        print('[debug] {identifier}|{last_mens_date_str}|{weeks}|{last_visit_date_str}|{curr_trimester}|{trimester_status}')

      for entry in response['entry']:
        patient_id         = ''
        identifier         = ''
        mobile             = ''
        mobile2            = ''
        name               = ''
        district           = ''
        puskesmas          = ''
        city               = ''
        last_mens_date_str = ''
        pregnancy_status   = ''
        weeks              = 0
        curr_trimester     = 0
        trimester_status   = ''
        encounter_resource = entry['resource']
        encounter_name     = self.get_encounter_name_from_encounter_resource(encounter_resource)
        
        if self.encounter_is_anc(encounter_name):
          if 'subject' in encounter_resource:
            if encounter_resource['subject']['type'] == 'Patient':
              patient_reference = encounter_resource['subject']['reference']
              patient_resource  = self.get_resource_by_reference(patient_reference)
              patient_id        = patient_resource['id']
              PNC_recordedDate  = self.get_condition_PNC_by_patient_id(patient_id)
              if not PNC_recordedDate:
                identifier        = self.get_mother_identifier_from_patient_resource(patient_resource)
                if identifier:
                  last_mens_date_str, weeks = self.get_last_mens_date_and_weeks_by_patient_id(patient_id)
  
                  if weeks:
                    curr_trimester = self.determine_trimester(weeks)
                    if curr_trimester < 0: 
                      trimester_status = 'INVALID'
                    else:
                      # if we check 4 weeks ago (trimester 1) it catch patient in trimester 1, 2, 3
                      # if we check 2 weeks ago (trimester 2) it catch patient in trimester 2, 3
                      # if we check 1 weeks ago (trimester 3) it only catch patient in trimester 3
                      if curr_trimester >= trimester:
                        trimester_status = 'Valid' 
                    
                  if trimester_status == 'Valid':
                    name             = self.get_patient_name_from_patient_resource(patient_resource)
                    mobile, mobile2  = self.get_patient_mobile_from_patient_resource(patient_resource)
                    district, city   = self.get_patient_city_from_patient_resource(patient_resource)
                    pregnancy_status = self.get_extension_patient_pregnancy_status(patient_resource)
                    puskesmas        = self.get_puskesmas_from_encounter_resource(encounter_resource)
                      
                    if puskesmas == '': puskesmas = district
  
                    member = dict()
                    member['patient_id']          = patient_id
                    member['identifier']          = identifier
                    member['mobile']              = mobile
                    member['mobile2']             = mobile2
                    member['name']                = name
                    member['puskesmas']           = puskesmas
                    member['city']                = city
                    member['last_mens_date_str']  = last_mens_date_str
                    member['last_visit_date_str'] = last_visit_date_str
                    member['weeks']               = weeks
                    member['curr_trimester']      = curr_trimester
                    member['pregnancy_status']    = pregnancy_status
                    member['trimester_status']    = trimester_status
                  
                    result.append(member)

        if self.debug:
          print(f'[debug] {identifier}|{last_mens_date_str}|{weeks}|{last_visit_date_str}|{curr_trimester}|{trimester_status}')
  
    else:
      print('Total 0 visit on '+ self.last_visit_date_str)

    return result
    
#----------------------------------------------------------------------------
  def wa_direct_send(self, mobile_number, patient_name):
    print(f"wa_direct_send('{mobile_number}', '{patient_name}')")

    if self.testing:
      mobile_number = self.hp_test_no
      patient_name  = 'Testing Ibu'      
      if mobile_number:
        return 'OK', 200
      else:
        return 'Failed', 500      
      
    if not mobile_number.startswith("62"):
      mobile_number = "62" + mobile_number
          
    data = {
      "to_name": patient_name,
      "to_number": mobile_number,
      "message_template_id": self.message_template_id,
      "channel_integration_id": self.wa_channel_integration_id,
      "language": {
        "code": "en"
      },
      "parameters": {
        "body": [
          {
            "key": "1",
            "value_text": patient_name,
            "value": "customer_name"
          },
          {
            "key": "2",
            "value_text": self.next_visit_date_str,
            "value": "next_visit_date"
          }
        ]
      }
    }
    
    # Prepare headers with bearer token
    headers = {
        "Authorization": self.wa_authorization
    }
    
    try:
        # Send the Qontak API Whatsapp POST request
        response = requests.post("https://service-chat.qontak.com/api/open/v1/broadcasts/whatsapp/direct", json=data, headers=headers)
        response.raise_for_status()
  
        if self.debug:
          print(f"[debug] SUCCESS phone:{mobile_number}, customer_name:{patient_name}, next_visit_date:{self.next_visit_date_str} sent to Qontak ANC Reminder")

        return f"Successfully sent request: {response.text}", 200
    except requests.exceptions.RequestException as e:
        if self.debug:
          print(f"[debug] ERROR phone:{mobile_number}, customer_name:{patient_name}, next_visit_date:{self.next_visit_date_str} exception:{str(e)}")

        return f"Error sending request: {str(e)}", response.json()['error']['code']
    
#----------------------------------------------------------------------------
  def collect_by_trimester(self, trimester=1):
    mother_data_list = self.get_all_mother_by_trimester(trimester)
    print('Total anc '+ str(len(mother_data_list)) +', valid if match trimester '+ str(trimester) +', reminder every '+ str(self.trimester_delta_weeks[trimester]) +' week(s)')

    row_num = len(self.worksheet.get_all_values())
    batch_data_list = []
    for mother_data in mother_data_list:
### cant do this since this script run one time every day ###################
#      if (self.write_limit > 0) and (self.write_count >= self.write_limit):
#        print('Break! write_limit reached, will continue in next round.')
#        break

      trimester_status = mother_data['trimester_status'] 
      if trimester_status:
        patient_id          = mother_data['patient_id']
        identifier          = mother_data['identifier']
        mobile              = mother_data['mobile']
        mobile2             = mother_data['mobile2'] 
        name                = mother_data['name']   
        puskesmas           = mother_data['puskesmas']
        city                = mother_data['city'] 
        last_mens_date_str  = mother_data['last_mens_date_str']
        weeks               = mother_data['weeks']
        last_visit_date_str = mother_data['last_visit_date_str']
        curr_trimester      = mother_data['curr_trimester']
        pregnancy_status    = mother_data['pregnancy_status']
        finalStatus         = 500
            
        print(f'mother_data: {mother_data}')
        
        if name not in self.sent_patient_name_list:
          if trimester_status == 'INVALID':
            self.create_report_list(patient_id, identifier, mobile2, name, puskesmas, city, last_mens_date_str, weeks, last_visit_date_str, curr_trimester, "INVALID")
          else:
            if identifier not in self.curr_patient_identifier_list:
              if curr_trimester == trimester: 
                if pregnancy_status == '':
                  if mobile:
                    if not self.check_wa_sent_from_log_by_patient_name('patient_name', name):
                      self.curr_patient_identifier_list.append(identifier)
                      result, finalStatus = self.wa_direct_send(mobile, name)   
                      self.create_report_list(patient_id, identifier, mobile, name, puskesmas, city, last_mens_date_str, weeks, last_visit_date_str, curr_trimester, finalStatus)
                      if finalStatus == 500 and mobile2:
                        result, finalStatus = self.wa_direct_send(mobile2, name)
                        self.create_report_list(patient_id, identifier, mobile2, name, puskesmas, city, last_mens_date_str, weeks, last_visit_date_str, curr_trimester, finalStatus)
                  else:
                    self.create_report_list(patient_id, identifier, mobile, name, puskesmas, city, last_mens_date_str, weeks, last_visit_date_str, curr_trimester)
                
              if self.report_list:
                batch_element = []
                batch_element.append(self.report_list)
                batch_data = dict()
                row_num += 1
                batch_data['range']  = f'A{row_num}:Q{row_num}'
                if self.testing: batch_data['range']  = f'A{row_num}:R{row_num}'
                batch_data['values'] = batch_element
                batch_data_list.append(batch_data)
                self.report_list = []

    if batch_data_list:
      self.worksheet.batch_update(batch_data_list)
            
#----------------------------------------------------------------------------
  def execute(self):
    self.get_wa_report_log(self.today_str)

    self.collect_by_trimester(1)  # send wa today for next visit (today + 4 days), last visit date 4 weeks ago
    self.collect_by_trimester(2)  # send wa today for next visit (today + 4 days), last visit date 2 weeks ago
    self.collect_by_trimester(3)  # send wa today for next visit (today + 4 days), last visit date 1 week ago


#===========================================================================
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def main_pubsub(cloud_event):
#if __name__ == "__main__":
  myReminder = WA_Reminder()    # default next_visit_date today +4 (YYYY-MM-DD)
  myReminder.testing = False     # default is True
  myReminder.debug   = False     # default is True
  myReminder.set_base_url_and_token('kobo')
  myReminder.execute()           # if param is blank then next visit date will be today + 4 days or .execute(today + 4 day)
  myReminder.set_base_url_and_token('kuapp')
  myReminder.execute()

#  result, finalStatus = myReminder.wa_direct_send('087876370598', 'TEST Mother')

#  print(myReminder.time_str)
#  print(myReminder.time_index)
#  last_mens_date_week = myReminder.get_last_mens_date_and_weeks_by_identifier('7000077912')
#  print(last_mens_date_week)
#  pregancy_week = myReminder.last_mens_date_to_pregnancy_weeks('2023-11-05')
#  print(pregancy_week)
#  last_mens_date = myReminder.get_last_mens_date_by_identifier('7000024260')
#  print(last_mens_date)

#  myTest = Pro_Reminder()
#  patient_resource = myTest.get_resource_by_identifier('7000086045', 'Patient')
#  patient_id = patient_resource['id']
#  print(patient_id)
#  PNC_recordedDate = myTest.get_condition_PNC_by_patient_id(patient_id)
#  print(PNC_recordedDate)


#  myTest = Pro_Reminder()
#  fin = open('anc_reminder_20241021.txt')
#  for line in fin.readlines():
#    rline = re.search('^(\d+)\|', line)
#    if rline:
#      encounter_id = rline.group(1)
#      encounter_resource = myTest.get_resource_by_id(encounter_id, 'Encounter')
#      patient_reference = encounter_resource['subject']['reference']
#      patient_resource = myTest.get_resource_by_reference(patient_reference)
#      patient_id = patient_resource['id']
#      last_mens_date, weeks = myTest.get_last_mens_date_and_weeks_by_patient_id(patient_id)
#      trimester = myTest.determine_trimester(weeks)
#      print(f'{encounter_id}|{patient_id}|{last_mens_date}|{weeks}|{trimester}')
#    else:
#      import sys
#      sys.stdout.write(line)
#  print(encounter_resource)
#
#  fin.close()

#  myTest = Pro_Reminder()
#  fin = open('anc_reminder_20241021_valid.txt')
#  for line in fin.readlines():
#    rline = re.search('^(\d+)\|(\d+)\|([^\|]*)\|(\d+)\|(.+)$', line)
#    if rline:
#      encounter_id = rline.group(1)
#      patient_id = rline.group(2)
#      last_mens_date = rline.group(3)
#      weeks = rline.group(4)
#      trimester = rline.group(5)
#      patient_resource = myTest.get_resource_by_id(patient_id, 'Patient')
#      name = myTest.get_patient_name_from_patient_resource(patient_resource)
#      phone_no = myTest.get_patient_mobile_from_patient_resource(patient_resource)
#      print(f'{encounter_id}|{patient_id}|{last_mens_date}|{weeks}|{trimester}|{name}|{phone_no}')
#    else:
#      import sys
#      sys.stdout.write(line)
#  print(encounter_resource)
#
#  fin.close()
  
