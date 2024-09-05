from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import json
import os

link = 'https://glints.com/vn/job-category/{}?page={}'

class Driver():
    def __init__(self, job_category):
        self.driver = webdriver.Chrome()
        self.failed = 0
        self.jobLinks = []
        self.job_category = job_category
        self.pages = self.getPagesNumber(job_category)

    def getPagesNumber(self, job_category):
        self.driver.get(link.format(job_category, 1))
        wait = WebDriverWait(self.driver, 10)
        elements = wait.until(EC.presence_of_all_elements_located((
                By.XPATH, "//div[starts-with(@class, 'AnchorPaginationsc__Pagination')]/a[last()-1]")))
        assert elements[0].text.isnumeric()
        return int(elements[0].text)

    def getJobLink(self, pages = None):
        pages = self.pages if pages is None else pages
        for i in range(1, pages+1):
            self.driver.get(link.format(self.job_category, i))
            wait = WebDriverWait(self.driver, 5)
            elements = wait.until(EC.presence_of_all_elements_located(
                    (By.XPATH, '//div[@role="presentation" and starts-with(@class, "JobCardsc__")]/a[starts-with(@class, "CompactOpportunityCardsc__")]')))

            for element in elements:
                    try:
                        self.jobLinks.append(element.get_attribute('href'))
                    except Exception:
                        self.failed += 1
                        pass

            time.sleep(6)

    def dump(self):
        directory = 'Data/{}'.format(self.job_category)
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, 'job_links.json')
        json.dump(self.jobLinks, open(file_path, 'w'), indent=4)

    def close(self):
        self.driver.close()

if __name__ == '__main__':
    job_category = 'computer-information-technology'
    driver = Driver(job_category)
    driver.getJobLink()
    driver.dump()
    print(driver.failed)
    driver.close()