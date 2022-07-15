import requests

url_1 = 'https://filedn.com/lAv70NoRi0O8LpU8BzbIRXJ/data/shi.csv'

r = requests.get(url_1)

print(r.status_code)
