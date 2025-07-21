import time
import csv
import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

CSV_OUTPUT = "unternehmen_bl_bs_ag.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class ZefixScraper:
    def __init__(self):
        options = Options()
        options.add_argument("--headless")
        self.driver = webdriver.Firefox(options=options)
        self.wait = WebDriverWait(self.driver, 10)
   
    def open_website(self):
        self.driver.get("https://www.zefix.ch/de/search/entity/welcome")

    def select_kantone(self, kantone):
        self.wait.until(EC.element_to_be_clickable((By.ID, "mat-mdc-slide-toggle-0"))).click()
        self.wait.until(EC.element_to_be_clickable((By.ID, "mat-select-1"))).click()

        for kanton in kantone:
            xpath = f"//div[contains(@class, 'cdk-overlay-pane')]//mat-option//small[contains(text(), '{kanton}')]"
            self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()

        self.exit_select()

    def select_rechtsform(self, rechtsform):
        self.wait.until(EC.element_to_be_clickable((By.ID, "mat-select-2"))).click()
        xpath = f"//div[contains(@class, 'cdk-overlay-pane')]//mat-option//span[contains(text(), '{rechtsform}')]"
        self.wait.until(EC.element_to_be_clickable((By.XPATH, xpath))).click()
        self.exit_select()

    def exit_select(self):
        self.wait.until(EC.element_to_be_clickable((By.TAG_NAME, "body"))).click()

    def submit_search(self):
        search_button = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
        self.driver.execute_script("arguments[0].click();", search_button)
        time.sleep(2)

    def set_entries_per_page(self):
        entries_dropdown = self.wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//div[contains(@class, 'mat-mdc-paginator')]//mat-select[@role='combobox']"
        )))
        self.driver.execute_script("arguments[0].scrollIntoView(true);", entries_dropdown)
        entries_dropdown.click()

        option_100 = self.wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//mat-option//span[normalize-space()='100']"
        )))
        self.driver.execute_script("arguments[0].click();", option_100)
        logger.info("Anzahl Einträge pro Seite auf 100 gesetzt.")

    def extract_rows(self):
        results = []
        table = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 6:
                try:
                    company_name = cells[0].text
                    uid = cells[2].text.split("\n")[0].strip()
                    sitz = cells[4].text
                    kanton = cells[5].text
                    results.append([company_name, uid, sitz, kanton])
                except Exception as e:
                    logger.warning("Fehler beim Parsen einer Zeile:", e)
                    continue
        return results

    def extract_data_from_pages(self):
        all_data = []
        logger.info("Extrahiere Einträge von Seite.")
        while True:
            all_data.extend(self.extract_rows())
            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Nächste Seite']")
                if next_button.get_attribute("aria-disabled") == "true":
                    break
                next_button.click()
                time.sleep(1)
            except:
                break
        return all_data
    
    def extract_rows(self):
        results = []
        table = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 6:
                try:
                    company_name = cells[0].text
                    uid = cells[2].text.split("\n")[0].strip()
                    sitz = cells[4].text
                    kanton = cells[5].text
                    results.append([company_name, uid, sitz, kanton])
                except Exception as e:
                    logger.warning("Fehler beim Parsen einer Zeile:", e)
                    continue
        return results

    def extract_data_from_pages(self):
        all_data = []
        logger.info("Extrahiere Einträge von Seite.")
        while True:
            all_data.extend(self.extract_rows())
            try:
                next_button = self.driver.find_element(By.CSS_SELECTOR, "button[aria-label='Nächste Seite']")
                if next_button.get_attribute("aria-disabled") == "true":
                    break
                next_button.click()
                time.sleep(1)
            except:
                break
        return all_data

    def quit(self):
         self.driver.quit()
    
def main():
    scraper = ZefixScraper()

    try:
        scraper.open_website()
        #scraper.select_kantone(["Basel-Stadt", "Basel-Landschaft"])
        scraper.select_kantone(["Appenzell I. Rh."])
        scraper.select_rechtsform("Aktiengesellschaft")
        scraper.submit_search()
        scraper.set_entries_per_page()

        data = scraper.extract_data_from_pages()

        with open(CSV_OUTPUT, mode="w", newline="", encoding="utf-8-sig") as file:
            writer = csv.writer(file)
            writer.writerow(["company_name", "uid", "sitz", "kanton"])
            writer.writerows(data)
        print(f"{len(data)} Einträge gespeichert in {CSV_OUTPUT}")
    finally:
        scraper.quit()

if __name__ == "__main__":
    main()
