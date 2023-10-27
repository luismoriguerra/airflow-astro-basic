from airflow.decorators import dag, task
from pendulum import datetime, timezone
from airflow.models import Variable
import requests

API = 'https://www.boredapi.com/api/activity/'

@dag(
    start_date=datetime(2023,1,1),
    schedule='@daily',
    tags=['acitivty'],
    catchup=False,   
)
def find_activity():
    @task()
    def get_activity():
        response = requests.get(API, timeout=10)
        return response.json()
    
    @task()
    def write_activity(response):
        filepath = Variable.get('activity_file')
        with open(filepath, 'a') as f:
            f.write(f"Today you will: {response['activity']}\r\n")
        return filepath
    
    @task()
    def read_activity_from_file(filepath):
        with open(filepath, 'r') as f:
            print(f.read())


    # get_activity() >> write_activity() >> read_activity_from_file()
    response = get_activity()
    filepath = write_activity(response)
    read_activity_from_file(filepath)

find_activity()