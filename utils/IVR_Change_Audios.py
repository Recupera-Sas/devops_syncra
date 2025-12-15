from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyautogui
import time
from datetime import datetime
import pandas as pd

def Change_Audio(data, API):
    
    if data is not None:
        try:
            chrome_options = Options()
            chrome_options.add_argument("--ignore-certificate-errors")  # Ignore certificate errors
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.execute_script("window.open('about:blank');")  # Open a blank tab for transactional processing
            driver.switch_to.window(driver.window_handles[0])  # Switch to the first tab (IVR login)
            
            def login_page(driver, url):
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                    pyautogui.write('recupera')
                    pyautogui.press('tab')
                    pyautogui.write('Recupera2025')
                    pyautogui.press('enter')
                    WebDriverWait(driver, 10).until(lambda d: d.current_url != url)
                except Exception as e:
                    print(f"Error en login en {url}: {e}")

            login_page(driver,"https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=999999")
            driver.switch_to.window(driver.window_handles[1])
            login_page(driver,"https://nextcallrecupera.controlnextapp.com/vicidial/admin.php")  # Navigate to TRANS URL

            now = datetime.now()
            print(f"{now}\n")
            print("CAMBIO DE AUDIOS EN PROGRESO\n")

            # Function to process lists from CSV data
            def process_list_from_csv(data):
                try:
                    # Read the CSV file
                    data = pd.read_csv(data, delimiter=';')  # Specify the delimiter as a semicolon
                    data.columns = data.columns.str.strip()  # Strip whitespace from column names
                    
                    for _, row in data.iterrows():
                        channel = row['CANAL'].strip().upper()
                        list_id = row['LISTA']
                        audio_value = row['AUDIO']
                        
                        campaign_name = str(row['CAMPAIGN'])  # Convert to string
                        if '.' in campaign_name:
                            campaign_name = campaign_name.split('.')[0]  # Split and take the first part
                        
                        if channel == "IVR":

                            print(f"IVR - Lista: {list_id} - Campaign: {campaign_name} - Audio: {audio_value}")
                            url = f"https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=3511&menu_id={campaign_name}"
                            driver.get(url)
                            change_audio_list(driver, audio_value)
                        elif channel == "TRANS":
                            print(f"TRANS - Lista: {list_id} - Campaign: {campaign_name} - Audio: {audio_value}")
                            url = f"https://nextcallrecupera.controlnextapp.com/vicidial/admin.php?ADD=3511&menu_id={campaign_name}"
                            driver.get(url)
                            change_audio_list(driver, audio_value)

                except FileNotFoundError as e:
                    print(f"Error: {e}. Please check the file path.")
                    return  # Exit the function if the file is not found

            # Function to change the audios
            def change_audio_list(driver, audio_value):
                try:
                    audiolist = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "menu_prompt"))
                    )
                    audiolist.clear()
                    audiolist.send_keys(audio_value)
                    time.sleep(1)
                    submit_button = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="admin_form"]/center/table/tbody/tr[16]/td/input'))
                    )
                    submit_button.click()
                except Exception as e:
                    print(f"Error performing actions: {e}")

            # Function to extract campaign name using XPath
            def extract_campaign_name(driver):
                try:
                    # Espera hasta que el elemento select esté presente
                    campaign_name_element = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[1]/table/tbody/tr[4]/td[2]/select'))
                    )
                    
                    # Encuentra el elemento option seleccionado
                    selected_option = campaign_name_element.find_element(By.XPATH, './option[@selected]')
                    campaign_name = selected_option.text.strip()
                    return campaign_name
                
                except Exception as e:
                    print(f"Error extracting campaign name: {e}")
                    return None

            # Call the function to process lists from CSV data
            process_list_from_csv(data)
            print("\nCAMBIO DE AUDIOS EXITOSO")
        finally:
            driver.quit()
    else:
        print("debug")