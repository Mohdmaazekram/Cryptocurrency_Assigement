import os
import pytz
import pandas as pd

from selenium import webdriver
from datetime import datetime
from scipy.stats import zscore
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import bigquery

def scrape_crypto_data():
    # Set up Chrome options for headless mode
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    # Initialize headless browser
    driver = webdriver.Chrome(options=options)

    driver.get("https://www.investing.com/crypto")

    wait = WebDriverWait(driver, 20)
    data_elem = wait.until(EC.presence_of_element_located(
        (By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[2]/div[1]/div[5]/div')
    ))

    raw_text = data_elem.text
    print(raw_text)
    driver.quit()


    # Parse scraped text
    lines = raw_text.split('\n')
    Other_column = lines[9:-1:2]
    Other_column = Other_column[1::]


    ist = pytz.timezone('Asia/Kolkata')
    Name = lines[10:-1:2]
    Symbol = [v.split()[0] for v in Other_column]
    Price_USD = [v.replace(',', '').split()[1] for v in Other_column]
    Vol_24H = [v.split()[2] for v in Other_column]
    Total_Vol = [v.split()[3] for v in Other_column]
    Chg_24H = [v.split()[4] for v in Other_column]
    Chg_7D = [v.split()[5] for v in Other_column]
    Market_Cap = [v.split()[6] for v in Other_column]


    df = pd.DataFrame({
        "Name": Name,
        "Symbol": Symbol,
        "Current_Price": Price_USD,
        "Volume_24_Hour": Vol_24H,
        "Total_Vol": Total_Vol,
        "Change_24_hours": Chg_24H,
        "Change_7_days": Chg_7D,
        "Market_Cap": Market_Cap
    })

    print("DataFrame Created SucessFuly")

    def convert_abbreviated_number(s):
        if isinstance(s, str):
            s = s.replace('$', '').replace(',', '').strip()
            if s[-1] in ['K', 'M', 'B', 'T']:
                num = float(s[:-1])
                if s[-1] == 'K':
                    return num * 1e3
                elif s[-1] == 'M':
                    return num * 1e6
                elif s[-1] == 'B':
                    return num * 1e9
                elif s[-1] == 'T':
                    return num * 1e12
            else:
                return float(s)
        return s

    df['datetime_ist'] = datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')
    df['Current_Price'] = df['Current_Price'].astype(float)
    df['Volume_24_Hour'] = df['Volume_24_Hour'].apply(convert_abbreviated_number)
    df['Market_Cap'] = df['Market_Cap'].apply(convert_abbreviated_number)
    df['Name_Symbol'] = df['Name'] + ' (' + df['Symbol'] + ')'
    df['Percentage_Change'] = df['Current_Price'].pct_change() * 100
    df['Z_Score_Price'] = zscore(df['Current_Price'])
    df['Rolling_Avg'] = df['Current_Price'].rolling(window=3).mean()
    df = df.fillna(0)
    df = df.head(10)
    print(df)
    push_to_bigquery(df)

def push_to_bigquery(df):
    client = bigquery.Client()
    table_id = "maazgsheet.crypto_currencies.crypto_currencies_data"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"✅ Data appended to BigQuery table: {table_id}")

if __name__ == "__main__":
    scrape_crypto_data()