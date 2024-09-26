from Extract.ExtractJobs import ExtractJobs
from Extract.ExtractJobDetail import ExtractJobDetail
from hdfsDriver.Driver import Driver
import json
import argparse
import re
import time
import random
from tqdm import tqdm

url = "http://wakuwa:9870"
user = "hadoop"
# path = "/recruiment/bronze/{}"
# source = "glints"

arg = argparse.ArgumentParser()
arg.add_argument("--jobCategory", type=str, default="computer-information-technology", required=False)
arg.add_argument("--source", type=str, default="glints", required=False)
arg.add_argument("--timeExecute", type=str)

args = arg.parse_args()
job_category = args.jobCategory
timeExecute = args.timeExecute.split('-') # YYYY-MM-DD
year = timeExecute[0]
month = timeExecute[1]
day = timeExecute[2]
### hdfs://wakuwa:9870/recruitment/bronze/glints/computer-information-technology/YYYY/MM/DD/{JobID}.json
path = "/recruitment/bronze/{}/{}/{}/{}/{}/".format(args.source, job_category, year, month, day)
partern = r'\/([^\/?]+)\?'
failed = []

if __name__ == "__main__":
    jobLinks = ExtractJobs(job_category)
    job = ExtractJobDetail(job_category)
    job.jobLinks = jobLinks.getJobLink()
    jobLinks.close()

    driver = Driver(url, user)
    for link in tqdm(job.jobLinks):
        try:
            id = re.search(partern, link).group(1)
            jobDetail = job.getJobDetail(link)
            jobDetail['jobDecription'] = job.getJobDescription(link)
            filePath = path + id + ".json"
            driver.write(filePath, jobDetail, 'json')
        except Exception:
            failed.append(link)
        finally:
            time.sleep(random.randint(3, 5))
            continue
    
    if len(failed) > 0:
        with open('logs/failed.json', 'w') as f:
            json.dump(failed, f)
    # driver.read(path, 'txt')
    job.close()
