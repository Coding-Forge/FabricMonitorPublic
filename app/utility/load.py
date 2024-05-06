import os
import json
import logging
import yaml
import msal
import aiohttp
from typing import Dict, Any, Coroutine
from dotenv import load_dotenv, dotenv_values
from datetime import datetime, timedelta
from app.utility.fabric import File_Table_Management
from datetime import datetime

logging.basicConfig(filename='myapp.log', level=logging.INFO)


class Load:

    def __init__(self):
        self.app_settings = dotenv_values("app/.env")

        print(f"App settings {self.app_settings.get('First_Name')}")

        for key, value in self.app_settings.items():
            print(f"{key} = {value}")