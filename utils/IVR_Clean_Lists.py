from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pyautogui
import pandas as pd

def login_page(driver):
    """Function to handle login."""
    time.sleep(5)  # Wait for the page to load
    pyautogui.write('recupera')
    pyautogui.press('tab')
    pyautogui.write('Recupera2025')
    pyautogui.press('enter')
    time.sleep(2)

def get_registers_from_list(driver):
    """Function to extract the number of registers from the page."""
    try:
        element = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[4]/table/tbody/tr[4]/td[2]/font'))
        )
        number_text = element.text
        number = int(number_text)
        return number
    except Exception as e:
        print(f"Error extracting number: {e}")
        return None

def click_download_button(driver):
    """Function to click the download button."""
    try:
        download_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/center/font/b/a[2]'))
        )
        download_button.click()
    except Exception as e:
        print(f"Error clicking download button: {e}")

def click_delete_database(driver):
    """Function to click the delete database button."""
    try:
        clean_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/center/font/b/a[4]'))
        )
        clean_button.click()
        time.sleep(2)
        clean_button_list = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/center/table[1]/tbody/tr[1]/td[2]/table/tbody/tr[4]/td/font/a'))
        )
        clean_button_list.click()
    except Exception as e:
        print(f"Error clicking delete database button: {e}")

def process_ivr_list(driver, list_id):
    """Function to process IVR lists."""
    driver.get(f"https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}")
    Registers = get_registers_from_list(driver)
    if Registers and Registers > 0:
        print(f"IVR - Lista: {list_id} - Registros: {Registers} - Estado: Descargado")
        time.sleep(5)
        click_download_button(driver)
        time.sleep(2)
        click_delete_database(driver)
    else:
        print(f"IVR - Lista: {list_id} - Registros: 0 - Estado: Sin base")

def process_trans_list(driver, list_id):
    """Function to process transactional lists."""
    driver.get(f"https://nextcallrecupera.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}")
    Registers = get_registers_from_list(driver)
    if Registers and Registers > 0:
        print(f"TRANS - Lista: {list_id} - Registros: {Registers} - Estado: Descargado")
        time.sleep(5)
        click_download_button(driver)
        time.sleep(2)
        click_delete_database(driver)
    else:
        print(f"TRANS - Lista: {list_id} - Registros: 0 - Estado: Sin base")

def Clean(data, API):
    """Main function to clean and download lists."""
    if data is not None:
        try:
            # Setup ChromeDriver service
            chrome_options = Options()
            chrome_options.add_argument("--ignore-certificate-errors")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("window.open('about:blank');")  # Open a blank tab for transactional processing
            
            # Login to IVR
            driver.switch_to.window(driver.window_handles[0])  # Switch to the first tab (IVR login)
            driver.get("https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=999999")
            login_page(driver)
            
            # Login to TRANS
            driver.switch_to.window(driver.window_handles[1])  # Switch to the second tab (TRANS login)
            driver.get("https://nextcallrecupera.controlnextapp.com/vicidial/admin.php")  # Navigate to TRANS URL
            login_page(driver)

            # Read CSV data
            data = pd.read_csv(data, delimiter=';')  # Specify the delimiter as a semicolon
            data.columns = data.columns.str.strip()  # Strip whitespace from column names
            
            for _, row in data.iterrows():
                channel = row['CANAL'].strip().upper()
                list_id = row['LISTA']
                if channel == "IVR":
                    driver.switch_to.window(driver.window_handles[0])  # Switch to IVR tab
                    process_ivr_list(driver, list_id)
                elif channel == "TRANS":
                    driver.switch_to.window(driver.window_handles[1])  # Switch to TRANS tab
                    process_trans_list(driver, list_id)

        finally:
            driver.quit()
    else:
        print("debug")