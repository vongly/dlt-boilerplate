from datetime import datetime, timezone
import time

import requests

BASE_URL = "https://api.stripe.com/v1"

class StripeCall():
    def __init__(
        self,
        api_secret: str,
        url: str = BASE_URL,
        test: bool = False,
        test_limit: int = 1,
    ):
    
        self.url = url
        self.headers = {
            "Authorization": f"Bearer { api_secret }"
        }

        self.test = test
        if test:
            if not test_limit or test_limit < 1:
                self.test_limit = 1
            else:
                self.test_limit = test_limit

    def yield_records(
        self,
        object: str,
        incremental: dict | None = None,
        processed_timestamp: datetime = datetime.now(timezone.utc),
    ):

        endpoint = f"{self.url}/{object}"

        # Checks for test parameters - limits query records
        if self.test:
            params = {"limit": self.test_limit}
        else:    
            params = {"limit": 100}

        # Checks for incremental parameters
        if incremental:
            if incremental["attribute"] and incremental["value"]:
                params[incremental["attribute"]] = incremental["value"]

        while True:
            response = requests.get(endpoint, headers=self.headers, params=params)
            if response.status_code != 200:
                print(f" Request failed: { response.status_code }, { response.text }")

            json_data = response.json()
            records = json_data.get("data", [])

            if not records:
                break
            for record in records:
                record["_dlt_processed"] = processed_timestamp
                yield record
            if not json_data.get("has_more"):
                break
            
            # Test run does not check for pagination
            if self.test:
                break

            params["starting_after"] = records[-1]["id"]
            time.sleep(0.5)

if __name__ == "__main__":
    pass