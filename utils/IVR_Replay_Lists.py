from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pyautogui
import time
from datetime import datetime

# Setup ChromeDriver service
chrome_options = Options()
chrome_options.add_argument("--ignore-certificate-errors")  # Ignore certificate errors
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

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
    4186, 4191, 4196, 5421, 5441, 5401, 4181, 5461, 5481, 
    5500, 5510, 5520  #, 4251
]
trans_modules = [
    "TRANS CLARO 1", "TRANS CLARO 2", "TRANS CLARO 3", "TRANS CLARO 4", 
    "TRANS CLARO 5", "TRANS CLARO 6", "TRANS CLARO 7", "TRANS CLARO 8", 
    "TRANS CLARO 9", "TRANS CLARO 10", "TRANS CLARO 11", "TRANS CLARO 12", 
    "TRANS CLARO 13", "TRANS CLARO 14", "TRANS CLARO 15", "TRANS PASH", "TRANS PUNTORED ACT", "TRANS PUNTO RED COB",
    "TEXT TO SPEECH 1", "TEXT TO SPEECH 2", "TEXT TO SPEECH 3"  #, "TRANS GMAC"
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
        number = 0
        return number

def get_registers_pending(driver, context):
    try:
        element = WebDriverWait(driver, 5).until(   
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[4]/table/tbody/tr[3]/td[3]/font'))
        )
        number_text = element.text
        number = int(number_text)
        return number
    except Exception as e:
        number = 0
        return number

def restart_list(driver, context):
    try:
        dropdown = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[1]/table/tbody/tr[6]/td[2]/select'))
        )
        dropdown.click()
        option = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[1]/table/tbody/tr[6]/td[2]/select/option[@value="Y"]'))
        )
        option.click()
        submit_button = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="vicidial_report"]/font/center[1]/table/tbody/tr[39]/td/input'))
        )
        submit_button.click()
        time.sleep(10)
    except Exception as e:
        pass

def process_ivr_list():
    driver.switch_to.window(driver.window_handles[1])  # Switch to the IVR tab
    Total_Registers = 0
    Total_Registers_Pending = 0
    Total_Calls = 0
    Restart_Lists = 0
    for index, list_id in enumerate(ivr_list_ids):
        url = f"https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}"
        driver.get(url)
        module = ivr_modules[index] if index < len(ivr_modules) else "Unknown Module"
        Registers = get_registers_from_list(driver)
        if Registers is not None and Registers > 10:
            Registers_Pending = get_registers_pending(driver, module)
            Calls = Registers - Registers_Pending
            if Registers > 10:
                Proces_List = "Rodando"
                if Registers_Pending < 10:
                    restart_list(driver, module)
                    Proces_List = "Reiniciada"
                    Restart_Lists += 1 
                time.sleep(0.5)
                print(f"{module} - Lista: {list_id} - Registros: {Registers} - Sin marcar: {Registers_Pending} - Marcado: {Calls} - Estado: {Proces_List}")
            else:
                print(f"{module} - Lista: {list_id} - Vacía")
        else:
            Registers = 0
            Registers_Pending = 0
            Calls = 0   
            print(f"{module} - Lista: {list_id} - Registros: 0 - Sin marcar: 0 - Estado: Sin base") 
        Total_Calls += Calls
        Total_Registers += Registers
        Total_Registers_Pending += Registers_Pending
    print(f"\nRegistros: {Total_Registers}\nLlamadas: {Total_Calls}\nPendientes: {Total_Registers_Pending}\nListas Reiniciadas: {Restart_Lists}\n\n")

def process_trans_list():
    driver.switch_to.window(driver.window_handles[0])  # Switch to the Transactional tab
    Total_Registers = 0
    Total_Registers_Pending = 0
    Total_Calls = 0
    Restart_Lists = 0
    for index, list_id in enumerate(trans_list_ids):
        if list_id in trans_list_ids and list_id != "4251":
            url = f"https://nextcallrecupera.controlnextapp.com/vicidial/admin.php?ADD=311&list_id={list_id}"
            driver.get(url)
            module = trans_modules[index] if index < len(trans_modules) else "Unknown Module"
            Registers = get_registers_from_list(driver)
            if Registers is not None and Registers > 10:
                Registers_Pending = get_registers_pending(driver, module)
                Calls = Registers - Registers_Pending
                if Registers > 10:
                    Proces_List = "Rodando"
                    if Registers_Pending < 10:
                        restart_list(driver, "TRANS")
                        Proces_List = "Reiniciada"
                        Restart_Lists += 1 
                    time.sleep(0.5)
                    print(f"{module} - Lista: {list_id} - Registros: {Registers} - Sin marcar: {Registers_Pending} - Marcado: {Calls} - Estado: {Proces_List}")
                else:
                    print(f"{module} - Lista: {list_id} - Vacía")
            else:
                Registers = 0
                Registers_Pending = 0
                Calls = 0
                print(f"{module} - Lista: {list_id} - Registros: 0 - Sin marcar: 0 - Estado: Sin base")
        Total_Calls += Calls
        Total_Registers += Registers
        Total_Registers_Pending += Registers_Pending
    print(f"\nRegistros: {Total_Registers}\nLlamadas: {Total_Calls}\nPendientes: {Total_Registers_Pending}\nListas Reiniciadas: {Restart_Lists}\n\n")

def is_within_time_range(current_time, start_time, end_time):
    current_minutes = current_time.hour * 60 + current_time.minute
    start_minutes = start_time[0] * 60 + start_time[1]
    end_minutes = end_time[0] * 60 + end_time[1]
    return start_minutes <= current_minutes < end_minutes

def login_page(driver):
    time.sleep(1)
    pyautogui.write('recupera')
    pyautogui.press('tab')
    pyautogui.write('Recupera2025')
    pyautogui.press('enter')
    time.sleep(2)

def send_summary_report(driver, phone_number: str, sent_count: int, not_sent_count: int, total_count: int, Phone: str):
    pending_count = total_count - (sent_count + not_sent_count)
    report_message = (f"{Phone}\n"
                      f"Enviados: {sent_count} - No enviados: {not_sent_count}\n"
                      f"Procesados: {sent_count + not_sent_count} - Pendientes: {pending_count}\n")
    driver.switch_to.window(driver.window_handles[2])  # Switch to WhatsApp tab
    driver.get("https://web.whatsapp.com/")
    time.sleep(15)  # Wait for QR code scanning
    search_box = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//div[@contenteditable="true"][@data-tab="3"]'))
    )
    search_box.click()
    search_box.send_keys(phone_number)
    time.sleep(2)  # Wait for search results
    contact = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, f'//span[@title="{phone_number}"]'))
    )
    contact.click()
    message_box = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//div[@contenteditable="true"][@data-tab="6"]'))
    )
    message_box.click()
    message_box.send_keys(report_message + Keys.ENTER)
    print("Resumen enviado correctamente.")

try:
    # Open the IVR and Transactional tabs
    driver.get('https://nextcallrecupera2.controlnextapp.com/vicidial/admin.php?ADD=999999')  # Open IVR base URL
    driver.execute_script("window.open('https://nextcallrecupera.controlnextapp.com/vicidial/admin.php');")  # Open Transactional base URL
    #driver.execute_script("window.open('https://web.whatsapp.com/');")  # Open WhatsApp Web in a new tab
    Count_Steep = 1
    print(f"\nReplay: {Count_Steep}")
    
    driver.switch_to.window(driver.window_handles[0])  # Switch to the first tab (IVR)
    login_page(driver)
    
    driver.switch_to.window(driver.window_handles[1])  # Switch to the second tab (Transactional)
    login_page(driver)
    
    # Switch to WhatsApp and wait for the user to log in
    #driver.switch_to.window(driver.window_handles[2])
    #time.sleep(25)  # Wait for the user to scan the QR code

    while True:
        now = datetime.now()
        print(f"{now}\n")
        print("CANAL - LIST_ID - BASE_MONTADA - PENDIENTES - LLAMADO - STATUS")
        
        # Define the start and end times for IVR and transactional
        start_time_ivr = (7, 0)
        end_time_ivr = (21, 0)
        if is_within_time_range(now, start_time_ivr, end_time_ivr):
            driver.switch_to.window(driver.window_handles[0])  # Switch to the IVR tab
            process_ivr_list()
        
        start_time_trans = (8, 0)
        end_time_trans = (19, 00)
        if is_within_time_range(now, start_time_trans, end_time_trans):
            driver.switch_to.window(driver.window_handles[1])  # Switch to the Transactional tab
            process_trans_list()
        
        # Send summary report (example usage)
        #send_summary_report(driver, '3180945484', 5, 2, 7, 'Resumen de Llamadas')
        
        Count_Steep += 1
        print(f"Replay: {Count_Steep}")
        time.sleep(10)  # Check every 10 seconds
finally:
    driver.quit()