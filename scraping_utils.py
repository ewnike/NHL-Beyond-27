"""
March 5, 2025.

Python utility file to gather
all similar functions needed for
the scraping files for this project.

Eric Winiecke.
"""

import time

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


def scroll_to_bottom(driver, wait_time: int = 2):  # noqa: D417
    """
    Scroll to the bottom of a dynamically loading web page.

    Args:
    ----
        driver (webdriver): Selenium WebDriver instance.
        wait_time (int): Time in seconds to wait between scrolls.

    Usage:
    ------
        driver.get(url)
        scroll_to_bottom(driver)

    """
    while True:
        previous_height = driver.execute_script("return document.body.scrollHeight")
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        time.sleep(wait_time)  # Allow new content to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == previous_height:
            break  # Exit when no more content loads