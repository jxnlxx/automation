#%% getstat.py

import json
import requests
import pandas as pd
import datetime as dt
from getstatcreds import stat_subdomain, stat_key #saved in env/lib/site-packages

stat_base_url = f"https://{stat_subdomain}.getstat.com/api/v2/{stat_key}"

def scrub(table_name):
    return table_name.replace(" ", "").replace("(","").replace(")","").replace(",","")

def dbize(folder_name):
    return folder_name.lower().replace(" ", "_").replace("(","").replace(")","").replace(",","") + "_ranks.db"

def first_monday(date):
    if date.weekday() == 0:
        return date
    else:
        return date + dt.timedelta(days=(7 - date.weekday()))

def first_sunday(date):
    if date.weekday() == 7:
        return date
    else:
        return date + dt.timedelta(days=(6 - date.weekday()))

def mon_weekspan(start_date, end_date):
    if start_date.weekday() == 0:
        mon_start = start_date
    else:
        mon_start = start_date + dt.timedelta(days=(7 - start_date.weekday()))
    while (end_date - mon_start).days >= 6:
        yield mon_start
        mon_start += dt.timedelta(days=7)

def sun_weekspan(start_date, end_date):
    if start_date.weekday() == 6:
        sun_start = start_date
    else:
        sun_start = start_date + dt.timedelta(days=(6 - start_date.weekday()))
    while (end_date - sun_start).days >= 6:
        yield sun_start
        sun_start += dt.timedelta(days=7)

def request_ranks(site_id, iso_date):
    url = f"{stat_base_url}/bulk/ranks?date={iso_date}&site_id={site_id}&engines=google&format=json"
    response = requests.get(url)
    response = response.json()
    job_id = response.get("Response").get("Result").get("Id")
    return job_id

def export_ranks(job_id):
    stream_url = f"/bulk_reports/stream_report/{job_id}"
    response = requests.get("https://iprospectman.getstat.com"+stream_url+f"?key={stat_key}")
    response = response.json()
    keywords = response.get("Response").get("Project").get("Site").get("Keyword")
    return keywords

def job_status(job_id):
    response = requests.get(f"{stat_base_url}/bulk/status?id={job_id}&format=json")
    response = response.json()
    status = response.get("Response").get("Result").get("Status")
    return status

def keywords_list(site_id):
    n = 1 # for showing progress through pagination
    keywords_list = f"/keywords/list?site_id={site_id}&results=5000&format=json"
    url = stat_base_url + keywords_list
    print(n, keywords_list)
    response = requests.get(url)
    response = response.json()
    keywords = response.get("Response").get("Result")
    while True:
        try:
            nextpage = response.get("Response").get("nextpage")
            url = stat_base_url + nextpage
            n += 1
            print(n, nextpage)
            response = requests.get(url)
            response = response.json()
            new_kws = response.get("Response").get("Result")
            keywords += new_kws
        except TypeError:
            break
    return keywords

def get_createdat(site_id):
    sites_all_url = f"{stat_base_url}/sites/all?&results=5000&format=json"
    response = requests.get(sites_all_url)
    response = response.json()
    created_at = response.get("Response").get("Result")
    created_at = pd.DataFrame(created_at)
    created_at = created_at.loc[created_at["Id"] == site_id, "CreatedAt"].iloc[0]
    return created_at

def bulk_list():
    # quick request to get the number of results
    bulk_list = "/bulk/list?results=1&format=json"
    url = stat_base_url + bulk_list
    response = requests.get(url)
    response = response.json()
    total_results = response.get("Response").get("totalresults")
    print(f"Total results: {total_results}")
    # start retrieving results
    n = 1 # for showing progress through pagination
    bulk_list = "/bulk/list?results=2000&format=json"
    url = stat_base_url + bulk_list
    print(n, bulk_list)
    response = requests.get(url)
    response = response.json()
    jobs_list = response.get("Response").get("Result")
    while True:
        try:
            nextpage = response.get("Response").get("nextpage")
            url = stat_base_url + nextpage
            n += 1
            print(n, nextpage)
            response = requests.get(url)
            response = response.json()
            new_jobs = response.get("Response").get("Result")
            jobs_list += new_jobs
        except TypeError:
            break
    return jobs_list

# %%
