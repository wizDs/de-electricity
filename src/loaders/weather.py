import requests
from typing import Dict, Optional

class DmiClient:

    def __init__(self, apikey: Optional[str], serviceversion: str = "v2"):
        self.apiKey = apikey
        self.servername = "https://dmigw.govcloud.dk"
        self.serviceversion = serviceversion

    def get_climate_data(
        self, 
        params: Dict[str, Optional[str]]
    ):
        url = self.get_baserequest("climateData", "stationValue")
        response = requests.get(url=url, params=params)

        return response.json()

    def get_metobs_data(
        self, 
        params: Dict[str, Optional[str]]
    ): 
        url = self.get_baserequest("metOps", "observation")
        response = requests.get(url=url, params=params)
        
        return response.json()
    

    def get_baserequest(
        self, 
        servicename: str,
        collection: str
    ) -> str:
        return f"{self.servername}/{self.serviceversion}/{servicename}/collections/{collection}/items"
    
    