from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tqdm import tqdm

import time
import json
import os
import random

class Driver():
    def __init__(self, job_category):
        self.driver = webdriver.Chrome()
        self.failed = []
        self.job_category = job_category
        self.jobLinks = []
        self.getJobLink(job_category)
        
    def getJobLink(self, job_category):
        linkpath = "Data/{}".format(job_category)
        assert os.path.exists(linkpath), "Directory not exist"

        for filename in os.listdir(linkpath):
            if filename.endswith(".json"):
                filepath = os.path.join(linkpath, filename)
                with open(filepath, "r") as f:
                    data = json.load(f)

                    if isinstance(data, list):
                        self.jobLinks.extend(data)
    
    # Job Title, Job Salary, Job Experience, Education, Location and Arrangement, Skills, Company & hirer name, Company links, Category, Company location
    def extractJobDetail(self, link):
        self.driver.get(link)
        wait = WebDriverWait(self.driver, 10)
        job = dict()

        jobHeader = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="TopFoldsc__JobOverviewContainer-sc-f7gyiq-2 fTcdeM"]//p')))
        skills = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="Skillssc__TagContainer-sc-gaj5e9-1 kwxkCn"]//span/div/p')))
        requirements = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="JobRequirementssc__TagsWrapper-sc-10j2bdt-1 ewBmwf"]//span/div/p')))
        hirer = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="HiringManagerSectionsc__CreatorInfoDetail-sc-ql38za-10 kJFJVW"]/div[1]/p')))
        company = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="HiringManagerSectionsc__CreatorInfoDetail-sc-ql38za-10 kJFJVW"]/div[2]/p/div')))
        companyLink = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="AboutCompanySectionsc__Main-sc-1cpbaxt-0 klxtwC"]/a')))
        category = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="JobCategoriessc__TagsWrapper-sc-1btru5u-1 bpLuhz"]//button//a')))
        location = wait.until(EC.presence_of_all_elements_located(
            (By.XPATH, '//div[@class="AboutCompanySectionsc__Main-sc-1cpbaxt-0 klxtwC"]/div[3]/p[2]')))
        # Job Link
        job["Link"] = link
        # Job Title
        job["Title"] = self.driver.find_elements(By.XPATH, '//div[@class="TopFoldsc__JobOverviewContainer-sc-f7gyiq-2 fTcdeM"]//p[1]')[0].text
        # Job Salary
        job["Salary"] = [i.text for i in jobHeader[1].find_elements(By.TAG_NAME, 'span')]
        # # Job Experience 
        # job["Experience"] = jobHeader[2].text
        # # Education
        # job["Education"] = jobHeader[3].text
        # Location and Arrangement
        job["Location"] = self.driver.find_elements(By.XPATH, '//div[@aria-label="Job Location and Arrangement"]/p[1]/div[2]')[0].text
        # Time
        job["Last_updated"] = jobHeader[-1].text 
        # CrawlTime
        job["Created_time"] = time.strftime("%d-%m-%Y %H:%M:%S", time.localtime())
        # Skills
        job["Skills"] = [skill.text for skill in skills if skill.text != ""]
        # Job Requirement
        job["Requirement"] = [requirement.text for requirement in requirements if requirement.text != ""]
        # Company & hirer name
        job["Hirer"] = hirer[0].text
        job["Company"] = company[0].text
        # Company links
        job["Company_link"] = companyLink[0].get_attribute("href")
        # Company location
        job["Company_location"] = self.driver.find_elements(By.XPATH, '//div[@class="AboutCompanySectionsc__Main-sc-1cpbaxt-0 klxtwC"]/div[3]/p[2]')[0].text
        # Category
        job["Category"] = [category.text for category in category if category.text != ""]
        
        return job
    
    def dumps(self):
        counter = 0
        path = "Data/{}/detail".format(self.job_category)
        os.makedirs(path, exist_ok=True)

        for link in tqdm(self.jobLinks):
            try:
                job = self.extractJobDetail(link)
                json.dump(job, open(path + "/job_" + str(counter) + ".json", "w", encoding="utf-8"), indent=4, ensure_ascii=False)
                counter += 1
            except:
                self.failed.append(link)
                print(link)
            finally:
                time.sleep(random.randint(3, 5))
                continue
    
    def close(self):
        self.driver.close()

if __name__ == "__main__":
    job_category = "computer-information-technology"
    driver = Driver(job_category)
    driver.dumps()
    print(driver.failed) if len(driver.failed) > 0 else print("Success")
    driver.close()