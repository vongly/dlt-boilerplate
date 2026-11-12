import simple_salesforce
import requests

import time, csv
from xml.etree import ElementTree as ET
from datetime import datetime, timezone


SF_LOGIN_URL='https://login.salesforce.com/services/Soap/u/59.0'

def simple_salesforce_call(
    username,
    password,
    security_token,
):
    simple_call = simple_salesforce.Salesforce(
        username=username,
        password=password,
        security_token=security_token,
    )

    return simple_call
    
class SalesforceBulkCall:
    def __init__(
        self,
        username,
        password,
        security_token,
        simple_call,
        login_url=SF_LOGIN_URL,
        test=False,
        test_limit=1,
    ):

        self.simple_call = simple_call
        self.username = username
        self.password = password
        self.security_token = security_token
        self.login_url = login_url
        self.test = test
        self.test_limit = test_limit
        
        soap_body = f'''
        <env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
        <env:Body>
            <n1:login xmlns:n1="urn:partner.soap.sforce.com">
            <n1:username>{username}</n1:username>
            <n1:password>{password}{security_token}</n1:password>
            </n1:login>
        </env:Body>
        </env:Envelope>
        '''

        headers = {'Content-Type': 'text/xml; charset=UTF-8', 'SOAPAction': 'login'}
        response = requests.post(login_url, data=soap_body, headers=headers)

        tree = ET.fromstring(response.text)
        server_url = tree.find('.//{urn:partner.soap.sforce.com}serverUrl').text
        instance_url = server_url.split('/services')[0]

        self.session_id = tree.find('.//{urn:partner.soap.sforce.com}sessionId').text
        self.job_url = f'{instance_url}/services/data/v59.0/jobs/query'

    def yield_records(
        self,
        object,
        incremental:dict=None,
        processed_timestamp=datetime.now(timezone.utc),
    ):
        print('\n', '  ', object)

        if incremental['attribute'] and incremental['value']:
            where_clause = f''' WHERE {incremental['attribute']} > {incremental['value']}'''
        else:
            where_clause = ''

        obj_desc = self.simple_call.__getattr__(object).describe()

        compound_fields = list(set([ field['compoundFieldName'] for field in obj_desc['fields'] if field['compoundFieldName'] is not None ]))
        fields = [ field['name'] for field in obj_desc['fields'] if field['name'] not in compound_fields ]

        query = f'select {', '.join(fields)} from {object}' + where_clause

        # Tests return default value of 1 record
        if self.test:
            if not self.test_limit or self.test_limit < 1:
                self.test_limit = 1
            query += f' LIMIT {self.test_limit}'

        job_response = requests.post(
            self.job_url,
            headers={'Authorization': f'Bearer {self.session_id}', 'Content-Type': 'application/json'},
            json={'operation': 'query', 'query': query}
        )

        job_id = job_response.json()['id']
        status_url = f'{self.job_url}/{job_id}'

        while True:
            response = requests.get(status_url, headers={'Authorization': f'Bearer {self.session_id}'})
            state = response.json()['state']
            print('      Job state:', state)
            if state in ['JobComplete', 'Failed', 'Aborted']:
                break
            time.sleep(2)

        if state != 'JobComplete':
            raise Exception('\n',f'     Job did not succeed: {state}')
        else:
            print('\n')

        result_url = f'{status_url}/results'

        with requests.get(result_url, headers={'Authorization': f'Bearer {self.session_id}'}, stream=True) as r:
            r.raise_for_status()
            lines = (line.decode('utf-8') for line in r.iter_lines(decode_unicode=False))
            reader = csv.DictReader(lines)

            for record in reader:
                record['_dlt_processed_utc'] = processed_timestamp
                yield record

if __name__ == '__main__':
    pass
