import asyncio
import json
import requests


from app.utility.helps import Bob


## This is an example of the results from the API call
# {
#   "value": [
#     {
#       "id": "1f69e798-5852-4fdd-ab01-33bb14b6e934",
#       "name": "My_Sample_Gateway",
#       "type": "Resource",
#       "publicKey": {
#         "exponent": "AQAB",
#         "modulus": "o6j2....cLk="
#       }
#     }
#   ]
# }


async def main():
    bob = Bob()
    settings = bob.get_settings()
    headers = bob.get_context()


    gateway_id = "b910099b-c9d9-4f90-b4cb-6a1bf61244cd"

    print(f'what is the headers {headers}')
# GET https://api.powerbi.com/v1.0/myorg/gateways
    
    response = requests.get('https://api.powerbi.com/v1.0/myorg/gateways', headers=headers)
    results = response.json()
    print(f'what is the results {results}')
    gateways = list()
    for gateway in results['value']:
        gateways.append(gateway.get("id"))


    response = requests.get(f'https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources', headers=headers)
    results = response.json()
    print(f'what is the results {results}')

if __name__ == "__main__":
    asyncio.run(main())