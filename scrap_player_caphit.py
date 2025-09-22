"""
August 11, 2024.

Script to scrape payer caphit, first name, and
last name from spotrac. Used selenium and set webdriver
to safari.

Eric Winiecke.
"""

import os

import pandas as pd
from bs4 import BeautifulSoup

# Import the shared scrolling function
from scraping_utils import scroll_to_bottom
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# Set up the Safari WebDriver
driver = webdriver.Safari()

# Base URL to scrape
BASE_URL = "https://www.spotrac.com/nhl/rankings/player/_/year/{}/sort/cap_total"

# Years to scrape
years = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

# Directory to store CSV files
OUTPUT_DIR = "player_cap_hits"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Dictionary to store DataFrames for each year
dfs_by_year = {}


def split_player_name(name):
    """Clean data and split player name."""
    name_parts = name.split()
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first_name, last_name


# Loop through each year and scrape the data
for year in years:
    url = BASE_URL.format(year)
    driver.get(url)

    # Wait until the table is loaded
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "list-group-item"))
    )

    # Call the shared scroll_to_bottom function
    scroll_to_bottom(driver, wait_time=2)

    # Get page source and parse with BeautifulSoup
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Find the elements containing the player names and cap hits
    player_names = []
    cap_hits = []

    for item in soup.find_all("li", class_="list-group-item"):
        name_div = item.find("div", class_="link")
        cap_hit_span = item.find("span", class_="medium")
        if name_div and cap_hit_span:
            name = name_div.text.strip()
            cap_hit = cap_hit_span.text.strip()
            player_names.append(name)
            cap_hits.append(cap_hit)

    # Create a DataFrame to store the results
    df = pd.DataFrame({"player name": player_names, "capHit": cap_hits})

    # Store the DataFrame in the dictionary
    dfs_by_year[year] = df

# Close the driver
driver.quit()

# Display the DataFrames for each year
for year, df in dfs_by_year.items():
    print(f"Data for {year}:")
    print(df.head(), "\n")

# Save the DataFrames to CSV files
for year, df in dfs_by_year.items():
    csv_path = os.path.join(OUTPUT_DIR, f"player_cap_hits_{year}.csv")
    df.to_csv(csv_path, index=False, mode="w")

print(f"Data saved to {OUTPUT_DIR} directory.")
