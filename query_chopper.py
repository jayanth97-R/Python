import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta



def tot_months(sd,ed):
    mnths =  (ed.year-sd.year)*12+ ed.month
    return mnths


def calculate_end_date():
    today = datetime.today()
    if today.day >= 25:
        end_date = today.replace(day=1) - timedelta(days=1)
    else:
        first_day_of_month = today.replace(day=1)
        end_date = first_day_of_month - timedelta(days=1)
        end_date = end_date.replace(day=1) - timedelta(days=1)
    return end_date

def calculate_start_date(end_date):
    start_date = end_date.replace(day=1, month=1, year=end_date.year-3)
    return start_date

def teradata_format_date(date):
    return date.strftime('%Y-%m-%d')

def generate_teradata_query(schema, table_name, filter_col, start_date, end_date):
    start_date_str = teradata_format_date(start_date)
    end_date_str = teradata_format_date(end_date)
    query = f"SELECT * FROM {schema}.{table_name} WHERE {filter_col} BETWEEN DATE '{start_date_str}' AND DATE '{end_date_str}'"
    return query

def generate_queries(schema, table_name, filter_col, start_date, end_date, interval):
    queries = []
    current_date = start_date
    flag = True
    while current_date < end_date and flag:
        query_start = current_date
        current_date,query_end = current_date + relativedelta(months=interval),(current_date + relativedelta(months=interval)) - timedelta(days=1)
        if query_end > end_date:
            query_end = end_date
            flag = False
        query = generate_teradata_query(schema, table_name, filter_col, query_start, query_end)
        queries.append({"schema": schema, "tblName": table_name, "query": query})
    return queries

json_data = '[{"schema":"schema1","tblName":"table1","filter_col":"date_column1"}]'
configurations = json.loads(json_data)

end_date = calculate_end_date()
start_date = calculate_start_date(end_date)

total_month = tot_months(start_date,end_date)

while True:
    interval = int(input("How many month's of data you want to pull in one iteration? "))
    if interval > total_month:
        print(f"Error: valid input range between {1} and {tot_months}")
    else:
        break
queries = []
for config in configurations:
    schema = config['schema']
    table_name = config['tblName']
    filter_col = config['filter_col']
    queries.extend(generate_queries(schema, table_name, filter_col, start_date, end_date,interval))

with open('queries.json', 'w') as f:
    json.dump(queries, f, indent=4)

print("Queries generated and saved to 'queries.json'")
