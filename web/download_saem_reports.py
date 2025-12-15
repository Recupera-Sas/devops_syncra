from PyQt6.QtWidgets import QMessageBox
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime, timedelta

# 📌 Configuration
login_url = "https://saemcolombia.com.co/recupera"
ivr_url = "https://saemcolombia.com.co/index.php/component/saem/?view=estado_ivr&id_campana={}&desde={}&hasta={}&tipo=Estandar&usuario=%"
sms_url = "https://saemcolombia.com.co/index.php/component/saem/?view=sms_campanas&layout=detalle_sms&id={}&e={}&c={}&desde={}&hasta={}&estado=%&usuario=%&tipo=Informativo"

# 📌 Get the first and last day of the current month
today = datetime.today()
first_day = today.replace(day=1).strftime('%Y-%m-%d')
last_day = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
last_day = last_day.strftime('%Y-%m-%d')

def start_driver():
    """ Initializes the Selenium WebDriver only when needed """
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    wait = WebDriverWait(driver, 20)
    return driver, wait

def login(driver, wait):
    """ Logs into SAEM using an existing driver """
    driver.get(login_url)
    wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="username"]')))
    driver.find_element(By.XPATH, '//*[@id="username"]').send_keys("recupera")
    driver.find_element(By.XPATH, '//*[@id="password"]').send_keys("Recupera2025#")
    print("Login successful. Waiting 15 seconds...")
    time.sleep(15)

def wait_for_single_tab(driver, timeout=10):
    """Cierra pestañas hasta dejar máximo 5 abiertas y selecciona la principal."""
    start = time.time()
    while len(driver.window_handles) > 5:
        # Cierra las pestañas adicionales (deja las primeras 5)
        for handle in driver.window_handles[5:]:
            driver.switch_to.window(handle)
            driver.close()
        driver.switch_to.window(driver.window_handles[0])
        if time.time() - start > timeout:
            print("Advertencia: No se pudieron cerrar todas las pestañas en el tiempo esperado.")
            break
        time.sleep(40)

def process_ivr(driver, wait, camp_id, wait_time):
    """ Processes IVR for a specific campaign """
    url = ivr_url.format(camp_id, first_day, last_day)
    driver.get(url)
    print(f"Loading IVR {camp_id}...")

    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/div/div[2]/div/div[4]/div[2]/a")))
        driver.find_element(By.XPATH, "/html/body/div[1]/main/div/div[2]/div/div[4]/div[2]/a").click()
        wait_for_single_tab(driver)
        print(f"IVR downloaded for ID {camp_id}")
    except:
        print(f"No download button found for ID {camp_id}")

    time.sleep(wait_time)

def process_sms(driver, wait, sms_id, executed, loaded, wait_time):
    """ Processes SMS for a specific campaign """
    executed = executed if pd.notna(executed) else ""  
    url = sms_url.format(sms_id, executed, loaded, first_day, last_day)
    driver.get(url)
    print(f"🔄 Loading SMS {sms_id}...")

    try:
        wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/main/div/div[2]/div/div[3]/a")))
        driver.find_element(By.XPATH, "/html/body/div[1]/main/div/div[2]/div/div[3]/a").click()
        wait_for_single_tab(driver)
        print(f"SMS downloaded for ID {sms_id}")
    except:
        print(f"No download button found for ID {sms_id}")

    time.sleep(wait_time)

def read_csv_lists_saem(csv_path):
    """ Reads a CSV and processes each row """
    df = pd.read_csv(csv_path, dtype=str, sep=';')
    wait_time = 10  

    driver, wait = start_driver()
    login(driver, wait)  # Log in once

    for _, row in df.iterrows():
        resource = row.get("RECURSO", "").strip().upper()

        if resource == "IVR":
            process_ivr(driver, wait, row["ID"], wait_time)
        elif resource == "SMS":
            process_sms(driver, wait, row["ID"], row["EJECUTADOS"], row["CARGADOS"], wait_time)
        else:
            print(f"Invalid resource for ID {row['ID']}: {resource}")

    print("Process completed.")
    
    driver.quit()
    
    Mbox_In_Process = QMessageBox() 
    Mbox_In_Process.setWindowTitle("")
    Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
    Mbox_In_Process.setText("Descarga de reportes ejecutada exitosamente.")
    Mbox_In_Process.exec()