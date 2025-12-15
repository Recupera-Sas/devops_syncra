import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pyautogui

def Uploads_DB(folder_path, API):
    chrome_options = Options()
    chrome_options.add_argument("--ignore-certificate-errors")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

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

    def upload_file(driver, file_path, destination_url):
        try:
            driver.get(destination_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(1) > td:nth-child(2) > input[type=file]"))
            )
            file_input = driver.find_element(By.CSS_SELECTOR, "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(1) > td:nth-child(2) > input[type=file]")
            file_input.send_keys(file_path)

            file_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]
            select_dropdown_item(driver, 
                                "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(2) > td:nth-child(2) > font > select", 
                                file_name_without_extension)

            select_dropdown_item(driver, 
                                "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > font > select", 
                                "57 - COL")
            
            click_radio_button(driver, 
                           "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(4) > td:nth-child(2) > font > input[type=radio]:nth-child(2)")

            select_fields_based_on_titles(driver)
            
            button = WebDriverWait(driver, 1000).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/table[2]/tbody/tr/td/form/table/tbody/tr[25]/th/input[1]"))
            )
            button.click()

            while True:
                try:
                    font_element = driver.find_element(By.XPATH, "/html/body/table[2]/tbody/tr/td/center/font")
                    content = font_element.text.lower()
                    print(f"File Uploaded: {file_path}")

                    if "hecho" in content or "done" in content :
                        break
                    else:
                        time.sleep(2)

                except Exception as e:
                    break

        except Exception as e:
            print(f"Error cargando archivo {os.path.basename(file_path)}: {e}")
            
    def select_fields_based_on_titles(driver):
        try:
            field_titles = [
                ("VENDOR LEAD CODE", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(2) > td:nth-child(2) > select"),
                ("SOURCE ID", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(3) > td:nth-child(2) > select"),
                ("PHONE NUMBER", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(4) > td:nth-child(2) > select"),
                ("TITLE", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(5) > td:nth-child(2) > select"),
                ("FIRST NAME", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(6) > td:nth-child(2) > select"),
                ("LAST NAME", "body > table:nth-child(7) > tbody > tr > td > form > table > tbody > tr:nth-child(8) > td:nth-child(2) > select")
            ]
            
            for title, xselector_select in field_titles:
                try:
                    select_element = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, xselector_select))
                    )
                    
                    options = select_element.find_elements(By.TAG_NAME, "option")
                    
                    for option in options:
                        if title.upper() in option.text.upper():
                            option.click()
                            break

                except Exception as e:
                    print(f"No se pudo seleccionar el campo {title} debido a: {e}")
        
        except Exception as e:
            print(f"Error al seleccionar los campos: {e}")

    def click_radio_button(driver, radio_selector):
        radio_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, radio_selector))
        )
        radio_button.click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "/html/body/table[2]/tbody/tr/td/form/table/tbody/tr[12]/td/input[1]"))
        )
        accept_button = driver.find_element(By.XPATH, "/html/body/table[2]/tbody/tr/td/form/table/tbody/tr[12]/td/input[1]")
        accept_button.click()

    def select_dropdown_item(driver, dropdown_selector, search_text):
        try:
            dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, dropdown_selector))
            )
            options = dropdown.find_elements(By.TAG_NAME, 'option')

            for option in options:
                if search_text.upper() in option.text.upper():
                    option.click()
                    return
        except Exception as e:
            print(f"Error seleccionando del menú desplegable: {e}")

    try:
        print(f"{datetime.now()}\nSUBIDA DE BASES EN PROGRESO\n")
        login_page(driver, "https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php")

        driver.execute_script("window.open('https://nextcallrecupera.controlnextapp.com/vicidial/admin.php');")
        driver.switch_to.window(driver.window_handles[1])
        login_page(driver, "https://nextcallrecupera.controlnextapp.com/vicidial/admin.php")

        files = sorted([f for f in os.listdir(folder_path) if f.endswith(('.csv', '.xlsx'))])
        for filename in files:
            file_path = os.path.join(folder_path, filename)
            destination_url = (
                "https://nextcallrecupera2.controlnextapp.com/vicidial/admin_listloader_fourth_gen.php"
                if "IVR" in filename.upper() else
                "https://nextcallrecupera.controlnextapp.com/vicidial/admin_listloader_fourth_gen.php"
            )

            if "nextcall" in destination_url:
                driver.switch_to.window(driver.window_handles[0])
            else:
                driver.switch_to.window(driver.window_handles[1])

            upload_file(driver, file_path, destination_url)
        time.sleep(5)
    finally:
        driver.quit()