import os
import csv
import time
from PyQt6.QtWidgets import QMessageBox, QFileDialog, QDialog, QVBoxLayout, QLabel
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common import exceptions as selexceptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys

def login_whatsapp(driver):
    """Log in to WhatsApp Web."""
    try:
        print("Logging into WhatsApp Web...")
        driver.get('https://web.whatsapp.com/')
        WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/div[3]/div/div[3]/header')))
        
        # Check for the button and click if it exists
        try:
            button = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/span[2]/div/div/div/div/div/div/div[2]/div/button')))
            button.click()
            print("Clicked the button before logging in.")
        except TimeoutException:
            print("The button was not found; proceeding without clicking.")
        
        print("Logged succesfully.")
        
    except selexceptions.TimeoutException:
        print("Error: Timeout while logging in.")
        driver.quit()
        raise

def validate_whatsapp(driver, phone_number, counter):
    """Check if the phone number has WhatsApp."""
    try:
        new_chat_btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="app"]/div[1]/div[3]/div/div[3]/header/header/div/span/div/div[1]/span/div/div/div[1]/div[1]/span')))
        driver.execute_script("arguments[0].scrollIntoView(true);", new_chat_btn)
        new_chat_btn.click()

        # Search box element
        search_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div[1]/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div/p')))
        
        # Clear the search box before entering the new phone number
        search_box.clear()
        search_box.send_keys(phone_number)
        time.sleep(0.5)

        # Wait for the element indicating a chat exists
        chat_exist = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CLASS_NAME, '_ak8l')))
        if chat_exist:
            chat_exist.click()
            print(f"{counter} Number {phone_number} has WhatsApp. ✅")
            return True
        else:
            print(f"{counter} Number {phone_number} does not have WhatsApp. ❌")
            return False
            
    # Add NoSuchElementException to the except clause!
    except (selexceptions.TimeoutException, selexceptions.ElementClickInterceptedException, NoSuchElementException):
        # Ensure search box is cleared if an error occurs
        try:
            search_box = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div[1]/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div/p')))
            search_box.send_keys(Keys.ESCAPE)
        except Exception as e:
            print(f"Could not clear the search box. Error")
            pass
            
        print(f"{counter} Number {phone_number} does not have WhatsApp (Exception). ❌")
        return False

def process_numbers(input_path, output_path, process):
    
    date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"{output_path}/Whatsapp Check Validate {date.format('%Y%m%d')}.csv"
    
    """Process phone numbers from a CSV file and validate if they have WhatsApp."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    login_whatsapp(driver)

    with open(input_path, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile, delimiter=';')

        # Check if the output file exists
        if not os.path.exists(output_path):
            with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow(["Number", "WhatsApp", "Date", "Time", "Hour"])

        counter = 0
        
        # Iterate over the numbers and validate
        for row in reader:
            counter += 1
            phone_number = row[0]  # Assuming the number is in the first column
            has_whatsapp = validate_whatsapp(driver, phone_number, counter)

            # Get the current date and time
            now = datetime.now()
            entry_date = now.strftime("%Y-%m-%d")
            time_with_minutes = now.strftime("%H:%M:%S")
            only_hour = now.strftime("%H")

            # Write results to the output file immediately
            with open(output_path, 'a', newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile, delimiter=';')
                writer.writerow([phone_number, "True" if has_whatsapp else "False", entry_date, time_with_minutes, only_hour])
        
        Mbox_In_Process = QMessageBox() 
        Mbox_In_Process.setWindowTitle("")
        Mbox_In_Process.setIcon(QMessageBox.Icon.Information)
        Mbox_In_Process.setText("Base de datos ejecutada con RPA exitosamente.")
        Mbox_In_Process.exec()

    driver.quit()
    print(f"Process completed. Results saved to: {output_path}")