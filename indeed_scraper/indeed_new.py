from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
# pip install cloudscraper
import cloudscraper

print(requests.__version__)

pages = [10, 20, 30, 40, 50, 60, 70]


titleList = []
companyList = []
locList = []
salList = []
descList = []


scraper = cloudscraper.create_scraper()

for page in pages:
    # change URL:
    url = f"https://www.indeed.com/jobs?q=Data%20Scientist&start={page}"

    source = scraper.get(url).text
    soup = BeautifulSoup(source, "html.parser")

    for jobs in soup.find_all(class_="result"):

        try:
            title = jobs.find("h2").find("a").text.strip()
        except Exception as e:
            title = None
        print("Title:", title)

        try:
            company = jobs.find("span", class_="companyName").text.strip()
        except Exception as e:
            company = None
        print("Company:", company)

        try:
            location = jobs.find("div", class_="companyLocation").text.strip()
        except Exception as e:
            location = None
        print("Location:", location)

        try:
            salary = jobs.find("span", class_="estimated-salary").text.strip()
        except Exception as e:
            salary = None
        print("Salary:", salary)

        
        link = jobs.find("h2").find("a")["href"]
        
        if "http" not in link:
            link = "http://www.indeed.com" + link
        print("Link:", link)

        data = scraper.get(link).text
        soup = BeautifulSoup(data, "html.parser")
        
        try:
            job_description = soup.find(class_="jobsearch-jobDescriptionText").get_text()
        except Exception as e:
            job_description = None
        print("job_description:", job_description)

        titleList.append(title)
        companyList.append(company)
        locList.append(location)
        salList.append(salary)
        # linkList.append(link)
        descList.append(job_description)

df = pd.DataFrame(
    {
        "Title": titleList,
        "Company": companyList,
        "Location": locList,
        "Salary": salList,
        #'Link':linkList,
        "Description": descList,
    }
)

print(df)
df.to_csv("indeed.csv", index=False)