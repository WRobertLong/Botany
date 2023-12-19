import subprocess
import random
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import mysql.connector
import logging
import yaml

# Load configurations from YAML file
with open('config.yml', 'r') as file:
    config = yaml.safe_load(file)

# Use the configurations
base_url = config['base_url']
max_pages = config['max_pages']
start_page = config['start_page']
vpn_retries = config['vpn_retries']
servers = config['vpn_servers']

# Extract the database configuration from loaded config
db_config = config['db_config']

# Configure logging
logging.basicConfig(filename='scraping_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def change_vpn_server(retries=3):
    while retries > 0:
        try:
            # Use the list of servers from the config file
            selected_server = random.choice(config['vpn_servers'])

            # Disconnect from the current VPN server
            subprocess.run(["nordvpn", "disconnect"], check=True)

            # Connect to the selected VPN server
            result = subprocess.run(
                ["nordvpn", "connect", selected_server], check=True)
            if result.returncode == 0:
                logging.info(f"Connected to VPN server: {selected_server}")
                return True
        except subprocess.CalledProcessError as e:
            logging.warning(
                f"Failed to connect to VPN server. Retrying... {retries} attempts left.")
            retries -= 1
            time.sleep(5)
    logging.error("Failed to establish VPN connection.")
    return False


# Database connection setup
db_config = {
    'host': 'localhost',
    'user': 'long',
    'password': 'gone',  # Replace with your actual password
    'database': 'CrossValidated'
}

def connect_to_database():
    return mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

def update_or_insert_user(user_id, username, url):
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        logging.info(
            f"Processing user {user_id}, username: {username}, url: {url}")

        cursor.execute("SELECT url FROM users WHERE userID = %s", (user_id,))
        result = cursor.fetchone()
        logging.info(f"Existing user check result: {result}")

        if result:
            if url and result[0] != url:
                logging.info("Updating existing user record.")
                cursor.execute("UPDATE users SET url = %s, timestamp = CURRENT_TIMESTAMP WHERE userID = %s",
                               (url, user_id))
        else:
            logging.info("Inserting new user record.")
            cursor.execute("INSERT INTO users (userID, username, url) VALUES (%s, %s, %s)",
                           (user_id, username, url))

        conn.commit()
        logging.info("Database operation committed.")
    except mysql.connector.Error as err:
        logging.error(f"SQL Error: {err}")
    except Exception as e:
        logging.error(f"Python Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# Connect to a random VPN server
servers = ["Europe", "Germany", "Spain", "The_Americas", "Netherlands"]
selected_server = random.choice(servers)
subprocess.run(["nordvpn", "connect", selected_server])


def scrape_page(base_url, start_page=44, max_pages=10, vpn_retries=10):
    user_data = []
    processed_users = set()

    for page in range(start_page, start_page + max_pages):
        vpn_attempts = 0
        while vpn_attempts < vpn_retries:
            if change_vpn_server():
                break
            vpn_attempts += 1
            logging.warning(f"VPN connection attempt {vpn_attempts} failed.")

        if vpn_attempts >= vpn_retries:
            logging.error(
                "Maximum VPN retries reached. Pausing for manual intervention.")
            input("Press Enter to continue after resolving the issue...")
            vpn_attempts = 0

        url = f"{base_url}&page={page}"
        logging.info(f"Scraping page: {url}")

        try:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                links_found = 0

                for link in soup.find_all('a', href=True):
                    if 'users' in link['href']:
                        user_id = link['href'].split('/')[-2]
                        if user_id not in processed_users:
                            processed_users.add(user_id)
                            scraped_data = scrape_profile(link['href'])

                            if scraped_data:
                                user_id, username, url = scraped_data
                                update_or_insert_user(user_id, username, url)
                                user_data.append(scraped_data)
                            links_found += 1

                logging.info(f"Links processed on page: {links_found}")

            elif response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                logging.warning(
                    f"Rate limited. Waiting for {retry_after} seconds.")
                time.sleep(retry_after)
                page -= 1

            else:
                logging.error(
                    f"Failed to fetch page, status code: {response.status_code}")

        except Exception as e:
            logging.error(f"An error occurred: {e}")

    logging.info(f"Total user data collected: {len(user_data)}")
    return user_data


def scrape_profile(url):
    if not url.startswith('http'):
        url = 'https://stats.stackexchange.com' + url
    parts = url.split('/')
    user_id = parts[-2]
    user_name = parts[-1]
    logging.info(f"Scraping user {user_id}, username: {user_name}")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            url_field = soup.find(
                'a', {'rel': 'me noreferrer', 'class': 'flex--item'})
            if url_field:
                return user_id, user_name, url_field['href']
    except Exception as e:
        logging.error(
            f"An error occurred while scraping the profile: {url}. Error: {e}")
    return user_id, user_name, None


# Start scraping
data = scrape_page(base_url, start_page, max_pages, vpn_retries)
