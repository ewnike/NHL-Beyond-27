"""
Scrape team payroll data from Spotrac using Selenium (Safari WebDriver).

Created on 2024-08-28 by Eric Winiecke.
"""

import os
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Set up the Safari WebDriver
driver = webdriver.Safari()

# Base URL to scrape
BASE_URL = "https://www.spotrac.com/nhl/cap/_/year/{}/sort/cap_maximum_space2"

# Years to scrape
years = [2015, 2016, 2017]

# Directory to store CSV files
OUTPUT_DIR = "team_salaries"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Loop through each year and scrape the data
for year in years:
    url = BASE_URL.format(year)
    driver.get(url)

    # Wait until the table is loaded
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "tablesorter-headerRow"))
    )

    # Scroll to the bottom of the page to load all content (if applicable)
    while True:
        previous_height = driver.execute_script("return document.body.scrollHeight")
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(2)  # Wait for new data to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == previous_height:
            break  # Exit the loop when no more new content is loaded

    # Get page source and parse with BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Find all rows in the table body
    rows = soup.find("tbody").find_all("tr")

    # Find the elements containing the team name and total player payroll
    team_names = []
    team_payroll = []

    # Iterate over each row to extract the name and payroll value
    for row in rows:
        # Get the team name, usually from the 'a' tag within the second 'td' element
        team_name_tag = row.find_all("td")[1].find("a")
        if team_name_tag:
            team_name = team_name_tag.get_text(strip=True)
        else:
            team_name = row.find_all("td")[1].get_text(strip=True)

        # Get the correct payroll column (assumed to be in the 7th column)
        payroll_column = row.find_all("td")[6].get_text(strip=True)

        team_names.append(team_name)
        team_payroll.append(payroll_column)

    # Create a DataFrame to store the results
    df = pd.DataFrame({"Team": team_names, "Total_Payroll": team_payroll})

    # Save DataFrame to a CSV file
    output_file = os.path.join(OUTPUT_DIR, f"team_salary_{year}.csv")
    df.to_csv(output_file, index=False)

# Close the driver
driver.quit()

print("Scraping completed. CSV files saved in the 'output' directory.")