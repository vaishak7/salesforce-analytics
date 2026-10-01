import os

from dotenv import load_dotenv
from simple_salesforce import Salesforce

load_dotenv()

sf = Salesforce(
    consumer_key=os.environ["SF_CONSUMER_KEY"],
    consumer_secret=os.environ["SF_CONSUMER_SECRET"],
    domain=os.environ["SF_DOMAIN"],

)
print("Connected to:", sf.sf_instance)

count = sf.query("SELECT COUNT() FROM Account")["totalSize"]
print("Total accounts:", count)

result = sf.query("SELECT Id, Name, Industry, SystemModstamp FROM Account LIMIT 5")
for record in result["records"]:
    print(record["Id"], "|", record["Name"], "|", record["Industry"], "|", record["SystemModstamp"])