import os
import boto3
import pandas as pd
import csv
import time
import subprocess
import logging
import asyncio
from pyppeteer import launch

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def format_word(word):
    return word.lower().replace(" ", "+")

async def check_for_button(page):
    try:
        accept = await page.querySelector('#bnp_btn_accept')
        if accept:
            await accept.click()
    except Exception as e:
        logging.warning("Button not found: %s", e)
        await page.screenshot({'path': 'button_not_found.png'})

def remove_linkedin(text):
    return text.replace(" | LinkedIn", "")

async def check_results(result, comp, page, pos):
    text = await (await result.getProperty('textContent')).jsonValue()
    person = {}
    parts = text.split(" - ")
    logging.info("Parts: %s", parts)
    if len(parts) > 2:
        if comp in format_word(parts[2]):
            link = await (await result.getProperty('href')).jsonValue()
            person["company"] = remove_linkedin(parts[2])
            person["position"] = parts[1]
            person["name"] = parts[0]
            person["account_url"] = link
            # Open the CSV file in append mode
            with open("data_output/output.csv", mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=["company", "position", "name", "account_url"],
                                        delimiter=';')
                writer.writerow(person)
            await asyncio.sleep(2)
    else:
        if len(parts) == 2:
            if comp in format_word(parts[1]):
                try:
                    div_element = await page.querySelector('.b_snippet')
                    # Find all strong elements within the located div
                    strong_elements = await div_element.querySelectorAll('strong')
                    for strong in strong_elements:
                        strong_text = await (await strong.getProperty('textContent')).jsonValue()
                        if pos in format_word(strong_text):
                            link = await (await result.getProperty('href')).jsonValue()
                            person["company"] = remove_linkedin(parts[1])
                            person["position"] = pos
                            person["name"] = parts[0]
                            person["account_url"] = link
                            # Open the CSV file in append mode
                            with open("data_output/output.csv", mode='a', newline='') as file:
                                writer = csv.DictWriter(file, fieldnames=["company", "position", "name", "account_url"],
                                                        delimiter=';')
                                writer.writerow(person)
                            await asyncio.sleep(2)
                            break
                except Exception as e:
                    logging.warning("Exception occurred: %s", e)
                    await page.screenshot({'path': 'exception_occurred.png'})

async def make_search(position, company):
    browser = await launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'])
    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36')

    pos = format_word(position)
    comp = format_word(company)

    await page.goto(f"https://www.bing.com/search?q={pos}+{comp}+linkedin+people")

    await check_for_button(page)

    try:
        li = await page.querySelector('li.b_pag')
        li_elements = await li.querySelectorAll('a')
    except Exception as e:
        logging.error("Exception occurred: %s", e)
        await page.screenshot({'path': 'b_pag_not_found.png'})
        await browser.close()
        return

    pages = [await (await li.getProperty('href')).jsonValue() for li in li_elements if li]
    if pages:
        pages.pop(0)

    results = await page.querySelectorAll('li.b_algo h2 a')
    logging.info(f'Search results for "{pos}":\n')

    for result in results:
        await asyncio.sleep(2)
        await check_results(result, comp, page, position)

    for page_url in pages:
        await page.goto(page_url)
        await check_for_button(page)
        results = await page.querySelectorAll('li.b_algo h2 a')
        logging.info(f'Search results for "{pos}":\n')

        for result in results:
            await asyncio.sleep(2)
            await check_results(result, comp, page, pos)

    await browser.close()

if __name__ == '__main__':
    bucket_name = 'dst-workbench'
    file_key_1 = 'mykyta/company_positions/companies_and_positions.csv'  # Replace with your actual file key
    file_key_2 = 'mykyta/company_positions/output.csv'

    # Read AWS credentials from environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_default_region = os.getenv('AWS_DEFAULT_REGION')

    # Initialize a session using Amazon S3
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      region_name=aws_default_region)

    # Download the file
    s3.download_file(bucket_name, file_key_1, 'companies_and_positions.csv')
    s3.download_file(bucket_name, file_key_2, 'data_output/output.csv')

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv('companies_and_positions.csv', delimiter=',')
    # Sort the DataFrame by the 'company' column
    df_sorted = df.sort_values(by='company')
    # df which is left
    global what_is_left
    what_is_left = df_sorted.copy()

    async def main():
        global what_is_left
        for index, row in df_sorted.iterrows():
            try:
                await make_search(row["position"], row["company"])
                # Drop the first row
                what_is_left = what_is_left.drop(what_is_left.index[0])
                # Reset the index if needed
                what_is_left = what_is_left.reset_index(drop=True)
            except Exception as e:
                logging.error("Exception occurred: %s", e)
                output_path = 'companies_and_positions.csv'
                what_is_left.to_csv(output_path, index=False)
                full_sync_query = f'''
                aws s3 cp companies_and_positions.csv s3://{bucket_name}/{file_key_1}
                aws s3 cp data_output/output.csv s3://{bucket_name}/{file_key_2}
                '''
                subprocess.call(full_sync_query, shell=True)
                break

        full_sync_query = f'''
                    aws s3 cp data_output/output.csv s3://{bucket_name}/{file_key_2}
                    '''
        subprocess.call(full_sync_query, shell=True)

    asyncio.run(main())
