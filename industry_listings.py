import json
import time
from datetime import datetime, timedelta
import yfinance as yf
import obj_str_access as os_access
import event_streams_access as es_access

def main():
    while True:
        industries = get_industry_perf()
        push_industry_perf(industries)
        time.sleep(7200)

def push_industry_perf(industries):
    os_access.create_text_file("mc-objstore", "industry-perf.json", industries)    
    push_event_message()

def push_event_message():
    driver = es_access.EventStreamsDriver('Industry-Perf', 'Inudstry-Perf', True)
    driver.run_task()

def get_industry_perf():
    industries = {
        "tech": {
            'companies': ["GOOGL", "AMZN", "AAPL", "MSI", "BB"],
            '5d-avg': 0,
        },
        "pharma": {
            'companies': ["PFE", "BAYN", "JNJ", "ROG.SW", "NOVN"],
            '5d-avg': 0,
        },
        "retail": {
            'companies': ["WMT", "COST", "KR", "HD", "WBA"],
            '5d-avg': 0,
        },
        "transport": {
            'companies': ["F", "GM", "VOW3.DE", "RNO.PA", "FCA"],
            '5d-avg': 0,
        },
        # "energy": []
    }

    for industry, values in industries.items():
        company_string = []
        for company in values['companies']:
            company_string.append(company + ' ')
        company_string = ''.join(company_string) 
        
        now = datetime.now()
        five_days_ago = datetime.now() - timedelta(days=5)
        data = yf.download(company_string, end=now.strftime("%Y-%m-%d"), start=five_days_ago.strftime("%Y-%m-%d"))

        close = data['Close']
        # Two means here to find the avergae of the past 5 days for each company and the mean of those 5 day averages
        values['5d-avg'] = close.mean().mean()

    return json.dumps(industries)

main()
