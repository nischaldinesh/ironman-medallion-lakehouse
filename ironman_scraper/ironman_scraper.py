from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import csv
import time
import re

def setup_driver(headless=False):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')

    driver = webdriver.Chrome(options=chrome_options)
    return driver


def select_event(driver, event_text):
    try:
        print(f"Selecting event: {event_text}")

        comboboxes = driver.find_elements(By.CSS_SELECTOR, "[role='combobox']")

        event_dropdown = None
        for cb in comboboxes:
            if 'IRONMAN' in cb.text or 'Championship' in cb.text:
                event_dropdown = cb
                break

        if not event_dropdown:
            try:
                event_dropdown = driver.find_element(By.CSS_SELECTOR, "[aria-label='Year'] [role='combobox']")
            except:
                pass

        if event_dropdown:
            current_text = event_dropdown.text
            print(f"    Found dropdown with text: '{current_text}'")

            if event_text.lower() in current_text.lower():
                print(f"    ✓ Already selected: '{current_text}'")
                return True

            try:
                first_athlete_before = driver.find_element(By.CSS_SELECTOR,
                                                           "div[data-rowindex='0'] div[data-field='athlete']").text
                print(f"    First athlete before: '{first_athlete_before[:30]}...'")
            except:
                first_athlete_before = None

            event_dropdown.click()
            time.sleep(2)


            options = driver.find_elements(By.CSS_SELECTOR, "[role='option']")
            print(f"    Found {len(options)} options")


            for opt in options:
                if event_text.lower() in opt.text.lower():
                    print(f"    Clicking option: '{opt.text}'")
                    opt.click()


                    print(f"    Waiting for data to refresh...")
                    max_wait = 15
                    waited = 0
                    data_changed = False

                    while waited < max_wait:
                        time.sleep(1)
                        waited += 1


                        try:
                            new_dropdown = driver.find_element(By.CSS_SELECTOR, "[aria-label='Year'] [role='combobox']")
                            if event_text.lower() in new_dropdown.text.lower():

                                if first_athlete_before:
                                    try:
                                        first_athlete_after = driver.find_element(By.CSS_SELECTOR,
                                                                                  "div[data-rowindex='0'] div[data-field='athlete']").text
                                        if first_athlete_after != first_athlete_before:
                                            print(f"    First athlete after: '{first_athlete_after[:30]}...'")
                                            data_changed = True
                                            break
                                    except:
                                        pass
                                else:

                                    time.sleep(2)
                                    data_changed = True
                                    break
                        except:
                            pass

                        print(f"    Still waiting... ({waited}s)")

                    if data_changed:
                        print(f"    Data refreshed successfully!")
                    else:
                        print(f"    Data may not have fully refreshed, continuing anyway...")
                        time.sleep(3)

                    return True


            print(f"    Could not find option containing: {event_text}")
            driver.find_element(By.TAG_NAME, 'body').click()
            time.sleep(0.5)
            return False
        else:
            print("    Could not find Event dropdown")
            return False

    except Exception as e:
        print(f"   Error selecting event: {e}")
        import traceback
        traceback.print_exc()
        return False


def parse_race_details_table(soup):

    details = {}


    detail_panel = soup.find('div', class_='MuiDataGrid-detailPanel')
    if not detail_panel:
        return details

    detail_rows = detail_panel.find_all('div', {'role': 'row', 'data-id': True})

    for row in detail_rows:
        row_id = row.get('data-id', '').lower()
        cells = row.find_all('div', {'role': 'gridcell'})

        if len(cells) >= 5:
            event_name = row_id

            if event_name in ['swim', 'bike', 'run']:
                details[f'{event_name}_time_detail'] = cells[1].get_text(strip=True)
                div_rank = cells[2].get_text(strip=True)
                gender_rank = cells[3].get_text(strip=True)
                overall_rank = cells[4].get_text(strip=True)

                details[f'{event_name}_div_rank'] = div_rank if div_rank and div_rank != '-' else ''
                details[f'{event_name}_gender_rank'] = gender_rank if gender_rank and gender_rank != '-' else ''
                details[f'{event_name}_overall_rank'] = overall_rank if overall_rank and overall_rank != '-' else ''

            elif 'transition' in event_name:
                key = event_name.replace(' ', '_')
                details[f'{key}_detail'] = cells[1].get_text(strip=True)

    return details


def extract_expanded_details(driver, row_index):
    try:
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.3)


        rows = driver.find_elements(By.CSS_SELECTOR, "div[role='row'][data-rowindex]")

        if row_index >= len(rows):
            return {}

        row = rows[row_index]
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", row)
        time.sleep(0.3)


        try:
            expand_button = row.find_element(By.CSS_SELECTOR, "button[aria-label='Expand']")
            expand_button.click()
        except:
            try:
                row.click()
            except:
                driver.execute_script("arguments[0].click();", row)

        time.sleep(1.5)


        soup = BeautifulSoup(driver.page_source, 'html.parser')
        details = {}


        detail_panel = soup.find('div', class_='MuiDataGrid-detailPanel')

        if detail_panel:
            info_boxes = detail_panel.find_all('div', class_=re.compile(r'css-1at62qq'))

            for box in info_boxes:
                h6_tags = box.find_all('h6')
                if len(h6_tags) >= 2:
                    value = h6_tags[0].get_text(strip=True)
                    label = h6_tags[1].get_text(strip=True).lower()

                    if 'div rank' in label:
                        details['div_rank'] = value
                    elif 'gender rank' in label:
                        details['gender_rank'] = value
                    elif 'overall rank' in label:
                        details['overall_rank'] = value
                    elif 'designation' in label:
                        details['designation'] = value
                    elif 'bib' in label:
                        details['bib'] = value
                    elif 'division' in label:
                        details['division'] = value
                    elif 'points' in label:
                        details['points'] = value


        if not details.get('div_rank'):
            page_text = soup.get_text(separator=' ')


            match = re.search(r'(\d+)\s*Div\s*Rank', page_text)
            if match:
                details['div_rank'] = match.group(1)

            match = re.search(r'(\d+)\s*Gender\s*Rank', page_text)
            if match:
                details['gender_rank'] = match.group(1)

            match = re.search(r'(\d+)\s*Overall\s*Rank', page_text)
            if match:
                details['overall_rank'] = match.group(1)


            match = re.search(r'(Finisher|DNF|DNS|DQ|DSQ)\s*Designation', page_text, re.IGNORECASE)
            if match:
                details['designation'] = match.group(1)


            match = re.search(r'(\d+)\s*Bib\b', page_text)
            if match:
                details['bib'] = match.group(1)


            match = re.search(r'([FM](?:PRO|\d{2}-\d{2}))\s*Division', page_text)
            if match:
                details['division'] = match.group(1)


            match = re.search(r'(\d+)\s*Points', page_text)
            if match:
                details['points'] = match.group(1)


        race_details = parse_race_details_table(soup)
        details.update(race_details)


        try:
            expand_button = row.find_element(By.CSS_SELECTOR,
                                             "button[aria-label='Collapse'], button[aria-expanded='true']")
            expand_button.click()
            time.sleep(0.3)
        except:
            try:
                row.click()
                time.sleep(0.3)
            except:
                pass

        return details

    except Exception as e:
        print(f" Error: {e}")
        return {}


def extract_basic_page_data(driver):

    soup = BeautifulSoup(driver.page_source, 'html.parser')


    rows = soup.find_all('div', {'role': 'row', 'data-rowindex': True})

    page_results = []

    for row in rows:
        result = {}
        cells = row.find_all('div', {'role': 'gridcell'})

        for cell in cells:
            field_name = cell.get('data-field', '')
            text = cell.get_text(strip=True)


            field_mapping = {
                'wtc_finishrankoverall': 'rank',
                'athlete': 'athlete_name',
                'wtc_swimtimeformatted': 'swim_time',
                'wtc_transition1timeformatted': 'transition_1',
                'wtc_biketimeformatted': 'bike_time',
                'wtc_transitiontime2formatted': 'transition_2',
                'wtc_runtimeformatted': 'run_time',
                'wtc_finishtimeformatted': 'finish_time',
            }

            if field_name in field_mapping:
                col_name = field_mapping[field_name]
                if text and text != '-':
                    result[col_name] = text


            if field_name == 'athlete':
                img = cell.find('img', alt=True)
                if img:
                    alt_text = img.get('alt', '')

                    if alt_text and alt_text not in ['user-avatar', 'no-avatar']:
                        result['country'] = alt_text.upper()

        if result.get('rank') or result.get('athlete_name'):
            page_results.append(result)

    return page_results


def scrape_page_with_expansion(driver, csv_writer, expand_details=True):

    print(f"    Extracting basic data...")
    basic_data = extract_basic_page_data(driver)
    row_count = len(basic_data)
    print(f"    Found {row_count} rows")

    if row_count == 0:
        return 0

    if expand_details:
        print(f"    Expanding rows for details...")

        for idx in range(row_count):
            print(f"      Row {idx + 1}/{row_count}...", end='')

            expanded = extract_expanded_details(driver, idx)
            basic_data[idx].update(expanded)


            if expanded:
                found_fields = [k for k, v in expanded.items() if v]
                if found_fields:
                    print(f" found: {len(found_fields)} fields", end='')


            csv_writer.writerow(basic_data[idx])
            print(f" ✓")
    else:
        for row_data in basic_data:
            csv_writer.writerow(row_data)

    return row_count


def scrape_all_pages(url, event_name, filename, expand_details=True, event_filter=None):
    driver = setup_driver(headless=False)
    total_results = 0

    csv_file = open(filename, 'w', newline='', encoding='utf-8')

    fieldnames = [
        'rank', 'athlete_name', 'country',
        'div_rank', 'gender_rank', 'overall_rank',
        'designation', 'bib', 'division', 'points',
        'swim_time', 'swim_time_detail', 'swim_div_rank', 'swim_gender_rank', 'swim_overall_rank',
        'transition_1', 'transition_1_detail',
        'bike_time', 'bike_time_detail', 'bike_div_rank', 'bike_gender_rank', 'bike_overall_rank',
        'transition_2', 'transition_2_detail',
        'run_time', 'run_time_detail', 'run_div_rank', 'run_gender_rank', 'run_overall_rank',
        'finish_time'
    ]

    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
    csv_writer.writeheader()
    csv_file.flush()

    try:
        print(f"\nScraping: {event_name}")
        print(f"URL: {url}")
        print(f"Writing to: {filename}")
        driver.get(url)
        time.sleep(8)

        if event_filter:
            if not select_event(driver, event_filter):
                print(f"  Warning: Could not select event filter '{event_filter}'")
            time.sleep(3)

        page_number = 1
        max_pages = 1000

        while page_number <= max_pages:
            print(f"\n  Page {page_number}:")

            page_count = scrape_page_with_expansion(driver, csv_writer, expand_details)
            csv_file.flush()

            if page_count > 0:
                total_results += page_count
                print(f"    Total so far: {total_results}")
            else:
                print("    No data found")
                break


            next_button = None
            next_selectors = [
                "button[aria-label='Go to next page']",
                "button[aria-label*='next' i]",
            ]

            for selector in next_selectors:
                try:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                    for button in buttons:
                        if button.is_displayed() and button.is_enabled():
                            disabled = button.get_attribute('disabled')
                            aria_disabled = button.get_attribute('aria-disabled')

                            if not disabled and aria_disabled != 'true':
                                next_button = button
                                break
                    if next_button:
                        break
                except:
                    continue

            if next_button:
                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                    time.sleep(0.5)
                    next_button.click()
                    print(f"    Moving to page {page_number + 1}...")
                    time.sleep(3)
                    page_number += 1
                except Exception as e:
                    print(f"    Error clicking next: {e}")
                    break
            else:
                print(f"    No more pages")
                break

        print(f"\n  ✓ Completed: {total_results} total results")

    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        csv_file.close()
        driver.quit()

    return total_results


if __name__ == "__main__":
    BASE_URL = "https://labs-v2.competitor.com/results/event/e798aa20-f278-e111-b16a-005056956277_Kona"

    events = {
        "2025_Women": ("2025 IRONMAN World Championship - Women", None),  # Default, no filter needed
        "2025_Men": ("2025 IRONMAN World Championship - Men", "2025 IRONMAN World Championship - Men"),
        "2024_Women": ("2024 IRONMAN World Championship - Women", "2024 IRONMAN World Championship - Women"),
        "2024_Men": ("2024 IRONMAN World Championship - Men", "2024 IRONMAN World Championship - Men"),
        "2023_Women": ("2023 IRONMAN World Championship - Women", "2023 IRONMAN World Championship - Women"),
        "2023_Men": ("2023 IRONMAN World Championship - Men", "2023 IRONMAN World Championship - Men"),
    }

    print("=" * 70)
    print("IRONMAN KONA COMPLETE RESULTS SCRAPER")
    print("=" * 70)
    print(f"Scraping {len(events)} event(s)")
    print("CSV files will update in real time as data is collected")
    print("=" * 70)

    for event_key, (event_display_name, event_filter) in events.items():
        filename = f"ironman_kona_{event_key.lower()}_complete_results.csv"
        total = scrape_all_pages(BASE_URL, event_display_name, filename, expand_details=True, event_filter=event_filter)
        print(f"\n✓ {event_key}: {total} results saved to {filename}")

    print("\n" + "=" * 70)
    print("ALL EVENTS COMPLETED")
    print("=" * 70)
