import logging
import azure.functions as func
import asyncio
from app.monitor import main as Monitor

app = func.FunctionApp()

# the function app runs at UTC time so you need to padd the time to your local time
@app.schedule(schedule="0 0 7 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def Fabric_Monitor(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    asyncio.run(Monitor())