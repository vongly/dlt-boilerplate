import requests

from datetime import datetime, timezone
import time


class ShopifyCall():
    def __init__(
        self,
        access_token,
        store_name,
        api_version='2025-10',
        test=False,
        test_limit=1,
    ):    

        self.url = f'https://{store_name}.myshopify.com/admin/api/{api_version}'
        self.headers = {
            'Content-Type': 'application/json',
            "Accept": "application/json",
            'X-Shopify-Access-Token': access_token,
        }

        self.test = test
        if test:
            if not test_limit or test_limit < 1:
                self.test_limit = 1
            else:
                self.test_limit = test_limit

    def yield_records(
        self,
        object_name,
        incremental:dict=None,
        processed_timestamp=datetime.now(timezone.utc),
    ):

        endpoint = f'{self.url}/{object_name}.json'

        # Checks for test parameters - limits query records
        # - may need to add 'status': 'any' -> products won't work with parameter
        if self.test:
            params = {'limit': self.test_limit}
        else:    
            params = {'limit': 250}

        # Checks for incremental parameters
        if incremental:
            if incremental['attribute'] and incremental['value']:
                params[incremental['attribute']] = incremental['value']

        seen_endpoints = set()

        while endpoint:

            if endpoint in seen_endpoints:
                # Try to extract the next page link directly
                last_response = locals().get('response')  # get last response if available
                if last_response:
                    link_header = last_response.headers.get('Link')
                    next_endpoint = None
                    if link_header:
                        parts = [p.strip() for p in link_header.split(',')]
                        for p in parts:
                            if 'rel="next"' in p:
                                next_endpoint = p[p.find('<') + 1 : p.find('>')]
                                break
                    endpoint = next_endpoint
                    continue
                else:
                    break

            seen_endpoints.add(endpoint)

            if 'page_info=' in endpoint:
                # Subsequent calls contain 'page_info=' in url endpoint -> retains the initial parameters
                # No need to restate parameters
                response = requests.get(endpoint, headers=self.headers)
            else:
                # Sets parameters for the first call
                response = requests.get(endpoint, headers=self.headers, params=params)

            if not response.ok:
                print("Error:", response.status_code, response.text)
                break

            try:
                records = response.json().get(object_name, [])
            except Exception:
                break

            for record in records:
                record['_dlt_processed'] = processed_timestamp
                yield record

            # Extracts the 'next' page from url endpoint
            link_header = response.headers.get('Link')
            next_endpoint = None
            if link_header:
                parts = [p.strip() for p in link_header.split(',')]
                for p in parts:
                    if 'rel="next"' in p:
                        next_endpoint = p[p.find('<') + 1 : p.find('>')]
                        break

            # Skips if next_endpoint is a duplicate
            if next_endpoint and next_endpoint in seen_endpoints:
                endpoint = None
                continue

            # Test run does not follow through with pagination
            if self.test:
                break

            endpoint = next_endpoint
            time.sleep(0.5)

if __name__ == '__main__':
    pass