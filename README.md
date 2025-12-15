# API Desktop AI

This project is a desktop application developed using PyQt6 and various data processing libraries. It integrates AI functionality with local system resources, making it a powerful tool for data analysis and interaction.

## 🚀 Features

- GUI built with PyQt6  
- Integration with PySpark for distributed data processing  
- Uses Selenium and WebDriver for web automation  
- System resource monitoring with `psutil`  
- File management and Excel support via `pandas` and `openpyxl`

---

## 🛠 Installation & Usage

Make sure you have **Python 3.9+** installed on your system.

```bash
# Clone the repository
git clone https://github.com/yourusername/API_Desktop_AI.git
cd API_Desktop_AI

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate        # On Linux/macOS
venv\Scripts\activate           # On Windows

# Install dependencies
pip install -r requirements.txt

# Run the application
python main.py

# Deactivate the environment when finished
deactivate
```

---

## 📦 Dependencies

```
PyQt6  
pyspark  
selenium  
webdriver_manager  
pandas  
openpyxl  
psutil  
```

---

## 📁 Project Structure

```
API_Desktop_AI/
│
├── main.py
├── config.py
├── requirements.txt
├── /folders/
│   └── custom scripts
```
