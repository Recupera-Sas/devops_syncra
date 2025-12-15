from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pyautogui
from datetime import datetime, timedelta

# IVR list of list_id values
ivr_list_ids = [
    5000, 5021, 5041, 5061, 5081, 5101, 5121, 5141, 5161, 5181, 
    5201, 5221, 5241, 5261, 5281, 5301, 5321, 5341, 5361, 5381, 
    5461
]

ivr_modules = [
    "IVR 1", "IVR 2", "IVR 3", "IVR 4", "IVR 5", "IVR 6", "IVR 7", 
    "IVR 8", "IVR 9", "IVR 10", "IVR 11", "IVR 12", "IVR 13", 
    "IVR 14", "IVR 15", "IVR 16", "IVR 17", "IVR 18", "IVR 19", 
    "IVR 20", "IVR 21"
]

# Transactional list of list_id values
trans_list_ids = [
    4000, 4021, 4041, 4061, 4081, 4101, 4121, 4141, 4161, 
    4186, 4191, 4251, 5421, 5441, 5401, 4196, 5461, 5481,
    5500, 5510, 5520, 4181
]

trans_modules = [
    "TRANS CLARO 1", "TRANS CLARO 2", "TRANS CLARO 3", "TRANS CLARO 4", 
    "TRANS CLARO 5", "TRANS CLARO 6", "TRANS CLARO 7", "TRANS CLARO 8", 
    "TRANS CLARO 9", "TRANS CLARO 10", "TRANS CLARO 11", "TRANS CLARO 12", 
    "TRANS CLARO 13", "TRANS CLARO 14", "TRANS CLARO 15", "TRANS GMAC",
    "TRANS PUNTORED ACT", "TRANS PUNTO RED COB", "TEXT TO SPEECH 1", 
    "TEXT TO SPEECH 2", "TEXT TO SPEECH 3", "TRANS PASH"
]

# Function to extract the number from the page
def get_registers_from_list(driver):
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

# Function to extract pending registers
def get_registers_pending(driver, context):
    try:
        if "IVR" in context:
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[2]/table/tbody/tr[11]/td[3]/font'))
            )
        else:
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[2]/table/tbody/tr[12]/td[3]/font'))
            )
        number_text = element.text
        number = int(number_text)
        return number
    except Exception as e:
        print(f"Error extracting pending number: {e}")
        return None

# Function to click download button
def click_download_button(driver):
    try:
        download_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/center/font/b/a[2]'))
        )
        download_button.click()
    except Exception as e:
        print(f"Error clicking download button: {e}")

# Function to process IVR lists
def process_ivr_list(driver):
    driver.get('https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=999999')  # Navigate to IVR base URL
    time.sleep(20)
    for index, list_id in enumerate(ivr_list_ids):
        url = f"https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}"
        driver.get(url)
        module = ivr_modules[index] if index < len(ivr_modules) else "Unknown Module"
        Registers = get_registers_from_list(driver)
        if Registers and Registers > 0:
            print(f"{module} - Lista: {list_id} - Registros: {Registers} - Estado: Con base")
            time.sleep(1)
            click_download_button(driver)
        else:
            print(f"{module} - Lista: {list_id} - Registros: 0 - Estado: Sin base")

# Function to process transactional lists
def process_trans_list(driver):
    driver.get('https://nextcallrecupera.controlnextapp.com/vicidial/admin.php')  # Navigate to transactional base URL
    time.sleep(20)
    for index, list_id in enumerate(trans_list_ids):
        url = f"https://nextcallrecupera.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}"
        driver.get(url)
        module = trans_modules[index] if index < len(trans_modules) else "Unknown Module"
        Registers = get_registers_from_list(driver)
        if Registers and Registers > 0:
            print(f"{module} - Lista: {list_id} - Registros: {Registers} - Estado: Con base")
            time.sleep(1)
            click_download_button(driver)
        else:
            print(f"{module} - Lista: {list_id} - Registros: 0 - Estado: Sin base")


def login_page(driver):

    time.sleep(5)

    pyautogui.write('recupera')
    pyautogui.press('tab')
    pyautogui.write('Recupera2025')
    pyautogui.press('enter')

    time.sleep(2)

def Function_Download(Value):
    if Value == "Run":
        
        try:
            
            # Setup ChromeDriver servicerece
            chrome_options = Options()
            chrome_options.add_argument("--ignore-certificate-errors")  # Ignora errores de certificados
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            # Open tabs and navigate to the base URLs
            
            driver.execute_script("window.open('about:blank');")  # Open a blank tab for transactional processing
            driver.switch_to.window(driver.window_handles[0])
            driver.get("https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=999999")
            driver.switch_to.window(driver.window_handles[1])
            driver.get("https://nextcallrecupera.controlnextapp.com/vicidial/admin.php")  # Navigate to TRANS URL

            driver.switch_to.window(driver.window_handles[0])  # Switch to the first tab (IVR)
            login_page(driver)
            driver.switch_to.window(driver.window_handles[1])  # Switch to the second tab (Transactional)
            login_page(driver)
            
            driver.switch_to.window(driver.window_handles[0])  # Switch to the first tab (IVR)
            process_ivr_list(driver)
            
            driver.switch_to.window(driver.window_handles[1])  # Switch to the second tab (Transactional)
            process_trans_list(driver)
            
            # while True:
                
            #     for i in range(1,20):

            #         time.sleep(60 * 58)

            #         driver.switch_to.window(driver.window_handles[0])  # Switch to IVR tab
            #         process_ivr_list(driver)

            #         driver.switch_to.window(driver.window_handles[1])  # Switch to Trans tab
            #         process_trans_list(driver)

        finally:
            driver.quit()
    
    else:
        pass