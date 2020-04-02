import json
from datetime import datetime, timedelta
import yfinance as yf
import obj_str_access as os_access
import event_streams_access as es_access

industries = {
    "tech": {
        'companies': [{"GOOGL": 0}, {"AMZN": 0}, {"AAPL": 0}, {"MSI": 0}, {"BB": 0}],
        '5d-avg': 0,
        'curr-avg': 0
    },
    "pharma": {
        'companies': [{"PFE": 0}, {"BAYN": 0}, {"JNJ": 0}, {"ROG.SW": 0}, {"NOVN": 0}],
        '5d-avg': 0,
        'curr-avg': 0
    },
    "retail": {
        'companies': [{"WMT": 0}, {"COST": 0},{ "KR": 0}, {"HD": 0}, {"WBA": 0}],
        '5d-avg': 0,
        'curr-avg': 0
    },
    "transport": {
        'companies': [{"F":0}, {"GM": 0}, {"VOW3.DE": 0}, {"RNO.PA": 0}, {"FCA": 0}],
        '5d-avg': 0,
        'curr-avg': 0
    },
    "energy": []
}

for industry, values in industries.items():
    avg_perf = 0
    # company_string = []
    # print(values)
    # for company in values['companies']:
    #     tick = list(company.keys())[0]
    #     # ticker = yf.Ticker(tick)
    #     # hist = ticker.history(period="5d")
    #     company_string.append(tick + ' ')

    # company_string = ''.join(company_string) 
    # print(company_string)
    now = datetime.now()
    five_days_ago = datetime.now() - timedelta(days=5)
    data = yf.download("GOOGL AMZN AAPL", start=now.strftime("%Y-%m-%d"), end=five_days_ago.strftime("%Y-%m-%d"), group_by='ticker')
    print(data)
    # hist = data.

# get_buckets()      
# os_access.create_text_file("mc-objstore-industry-perf", "industry-perf.json", json.dumps(industries))    
