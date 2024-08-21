# import random
# import time
# import logging
# import pandas as pd
# import pymysql
# import requests
# from scrapy.http import TextResponse
# from datetime import datetime
# import os
#
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
#
# # df_store_ids = pd.read_csv(r"/QC/input/zepto_store_ids.csv", index_col=False)
#
# delivery_date = datetime.today().strftime("%Y%m%d")
# browser_list = ["chrome100", "chrome104", "chrome110", "edge99", "edge101", "safari15_3", "safari15_5", "chrome116"]
#
# page_save_pdp = f'D:/pankaj_page_save/{delivery_date}/zepto/HTMLS/'
#
# host = 'localhost'
# user = 'root'
# password = 'actowiz'
# database = 'qcg'
#
# try:
#     connection = pymysql.connect(host=host, user=user, password=password, database=database)
#     with connection.cursor() as cursor:
#         cursor.execute("SELECT * FROM input_zepto_master_file")
#
#         create_table_query = f"""
#             CREATE TABLE IF NOT EXISTS `zepto_{delivery_date}` (
#                 `Id` INT NOT NULL AUTO_INCREMENT,
#                 `comp` VARCHAR(255) DEFAULT 'N/A',
#                 `fk_id` VARCHAR(255) DEFAULT 'N/A',
#                 `pincode` VARCHAR(255) DEFAULT 'N/A',
#                 `url` VARCHAR(255) DEFAULT 'N/A',
#                 `name` VARCHAR(255) DEFAULT 'N/A',
#                 `availability` VARCHAR(255) DEFAULT 'N/A',
#                 `price` VARCHAR(255) DEFAULT 'N/A',
#                 `discount` VARCHAR(255) DEFAULT 'N/A',
#                 `mrp` VARCHAR(255) DEFAULT 'N/A',
#                 `slug` VARCHAR(255) DEFAULT 'N/A',
#                 PRIMARY KEY (`Id`),
#                 UNIQUE KEY `fid` (`fk_id`, `pincode`)
#             ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
#             """
#         cursor.execute(create_table_query)
# except pymysql.MySQLError as e:
#     logger.error(f"Error connecting to the database: {e}")
#     raise
#
# def fetch_with_retries(url, cookies, headers, impersonate, retries=3, delay=5):
#     for attempt in range(retries):
#         try:
#             reqq = requests.get(
#                 url,
#                 cookies=cookies,
#                 headers=headers,
#                 proxies={"http": None, "https": None},
#                 timeout=10
#             )
#             return reqq
#         except Exception as e:
#             logger.error(f"Request to {url} failed on attempt {attempt + 1} with error: {e}")
#             if attempt < retries - 1:
#                 time.sleep(delay)
#             else:
#                 raise e
#
# def scraper(url, pincode, pid, response,id, statuss, page_save_pdp, slug, comp="zepto"):
#     if not os.path.exists(page_save_pdp):
#         os.makedirs(page_save_pdp)
#     # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
#     open(page_save_pdp + pid + pincode + ".html", "w", encoding="utf-8").write(response.text)
#
#     if statuss != 200:
#         logger.info(f"Processing link No {id} with status {statuss}")
#
#         update_query = """
#                         UPDATE input_zepto_master_file SET status = %s WHERE slug = %s
#                     """
#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(update_query, (statuss, slug))
#             connection.commit()
#
#             logger.info(f"Processed link No {id} with updated Status {statuss}")
#
#         except Exception as e:
#             logger.error(f"Error inserting data into the database: {e}")
#     else:
#         item = {
#             'url': url,
#             'pincode': pincode,
#             'comp': comp,
#             'fk_id': pid,
#             'name': response.xpath("//h1/text()").get() or None,
#             'mrp': response.xpath("//*[@data-testid='pdp-discounted-price']/text()").get().strip("₹") if response.xpath("//*[@data-testid='pdp-discounted-price']/text()") else None,
#             'price': response.xpath("//*[@data-test-id='pdp-selling-price']/text()").get().strip("₹") if response.xpath("//*[@data-test-id='pdp-selling-price']/text()") else None,
#             'discount': response.xpath("//div[contains(text(),'% Off')]/text()").get().strip("% Off") if response.xpath("//div[contains(text(),'% Off')]/text()") else None,
#             'availability': False if 'Out of Stock' in response.text or not response.xpath("//h1/text()").get() else True,
#             'slug':slug
#         }
#
#         logger.info(f"Processing link No {id} with status {statuss}")
#
#         insert_query = f"""
#             INSERT INTO `zepto_{delivery_date}` (comp, fk_id, pincode, url, name, availability, price, discount, mrp, slug)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON DUPLICATE KEY UPDATE
#                 name=VALUES(name),
#                 availability=VALUES(availability),
#                 price=VALUES(price),
#                 discount=VALUES(discount),
#                 mrp=VALUES(mrp);
#             """
#         try:
#             with connection.cursor() as cursor:
#                 cursor.execute(insert_query, (
#                     item['comp'],
#                     item['fk_id'],
#                     item['pincode'],
#                     item['url'],
#                     item['name'],
#                     item['availability'],
#                     item['price'],
#                     item['discount'],
#                     item['mrp'],
#                     item['slug']
#                 ))
#             connection.commit()
#
#             update_query = """
#                             UPDATE input_zepto_master_file SET status = %s WHERE slug = %s
#                         """
#             try:
#                 with connection.cursor() as cursor:
#                     cursor.execute(update_query, ("Done", slug))
#                 connection.commit()
#
#                 logger.info(f"Processed link No {id} with updated Status Done")
#
#             except Exception as e:
#                 logger.error(f"Error inserting data into the database: {e}")
#         except Exception as e:
#             logger.error(f"Error inserting data into the database: {e}")
#
# # Main scraping logic
# try:
#     with connection.cursor() as cursor:
#         cursor.execute("SELECT * FROM input_zepto_master_file where status != 'Done'")
#         results = cursor.fetchall()
#
#     for links in results:
#         cookies = {
#             'store_id': str(links[6]),
#             'latitude': str(links[8]),
#             'longitude': str(links[9])
#         }
#
#         headers = {
#             'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#             'accept-language': 'en-US,en;q=0.9',
#             'sec-ch-ua-platform': '"Windows"',
#             'sec-fetch-dest': 'document',
#             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
#         }
#
#         id = str(links[0])
#         pid = str(links[1])
#         url = str(links[2])
#         slug = str(links[10])
#
#         try:
#             reqq = fetch_with_retries(
#                 url,
#                 cookies=cookies,
#                 headers=headers,
#                 impersonate=random.choice(browser_list)
#             )
#             statuss = reqq.status_code
#             response = TextResponse(url=reqq.url, body=reqq.text, encoding='utf-8')
#             pincode = str(links[3])
#             scraper(url, pincode, pid, response,id, statuss, page_save_pdp, slug)
#
#         except Exception as e:
#             logger.error(f"Failed to process URL {url} with error: {e}")
#
# finally:
#     connection.close()




# ----------------------------------------------------------------------------------------------------------------------


import random
import sys
import time
import logging
import pandas as pd
import pymysql
from curl_cffi import requests
from scrapy.http import TextResponse
from datetime import datetime
import os
from sys import argv


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
delivery_date = datetime.today().strftime("%Y%m%d")
browser_list = ["chrome100", "chrome104", "chrome110", "edge99", "edge101", "safari15_3", "safari15_5", "chrome116"]
page_save_pdp = f'D:/pankaj_page_save/{delivery_date}/zepto/HTMLS/'

# Database credentials
host = 'localhost'
user = 'root'
password = 'actowiz'
database = 'qcg'

# Create database connection
try:
    connection = pymysql.connect(host=host, user=user, password=password, database=database)
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM input_zepto_master_file")

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `zepto_{delivery_date}` (
                `Id` INT NOT NULL AUTO_INCREMENT,
                `comp` VARCHAR(255) DEFAULT 'N/A',
                `fk_id` VARCHAR(255) DEFAULT 'N/A',
                `pincode` VARCHAR(255) DEFAULT 'N/A',
                `url` VARCHAR(255) DEFAULT 'N/A',
                `name` VARCHAR(255) DEFAULT 'N/A',
                `availability` VARCHAR(255) DEFAULT 'N/A',
                `price` VARCHAR(255) DEFAULT 'N/A',
                `discount` VARCHAR(255) DEFAULT 'N/A',
                `mrp` VARCHAR(255) DEFAULT 'N/A',
                `slug` VARCHAR(255) DEFAULT 'N/A',
                PRIMARY KEY (`Id`),
                UNIQUE KEY `fid` (`fk_id`, `pincode`)
            ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
            """
        cursor.execute(create_table_query)
except pymysql.MySQLError as e:
    logger.error(f"Error connecting to the database: {e}")
    raise

def fetch_with_retries(url, cookies, headers, impersonate, retries=3, delay=5):
    # for attempt in range(retries):
    try:
        reqq = requests.get(
            url,
            cookies=cookies,
            headers=headers,
            proxies={"http": None, "https": None},
            timeout=3,
            impersonate=impersonate
        )
        return reqq
    except Exception as e:
        # logger.error(f"Request to {url} failed on attempt {attempt + 1} with error: {e}")
        # if attempt < retries - 1:
        #     time.sleep(delay)
        # else:
        raise e

def scraper(url, pincode, pid, response, id, status_code, page_save_pdp, slug, comp="zepto"):
    if not os.path.exists(page_save_pdp):
        os.makedirs(page_save_pdp)

    file_path = os.path.join(page_save_pdp, f"{pid}_{pincode}.html")
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(response.text)

    if status_code != 200:
        logger.info(f"Processing link No {id} with status {status_code}")

        update_query = """
            UPDATE input_zepto_master_file SET status = %s WHERE slug = %s
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(update_query, (status_code, slug))
            connection.commit()
            logger.info(f"Processed link No {id} with updated Status {status_code}")
        except pymysql.MySQLError as e:
            logger.error(f"Database error during status update: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during status update: {e}")
    else:
        item = {
            'url': url,
            'pincode': pincode,
            'comp': comp,
            'fk_id': pid,
            'name': response.xpath("//h1/text()").get() or None,
            'mrp': response.xpath("//*[@data-testid='pdp-discounted-price']/text()").get().strip("₹") if response.xpath("//*[@data-testid='pdp-discounted-price']/text()") else None,
            'price': response.xpath("//*[@data-test-id='pdp-selling-price']/text()").get().strip("₹") if response.xpath("//*[@data-test-id='pdp-selling-price']/text()") else None,
            'discount': response.xpath("//div[contains(text(),'% Off')]/text()").get().strip("% Off") if response.xpath("//div[contains(text(),'% Off')]/text()") else None,
            'availability': 'Out of Stock' not in response.text and response.xpath("//h1/text()").get() is not None,
            'slug': slug
        }

        if not all([item['price'], item['mrp'], item['name']]):
            logger.warning(f"Missing data for URL: {url}, pincode: {pincode}")
            return

        logger.info(f"Processing link No {id} with status {status_code}")

        insert_query = f"""
            INSERT INTO `zepto_{delivery_date}` (comp, fk_id, pincode, url, name, availability, price, discount, mrp, slug)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name=VALUES(name),
                availability=VALUES(availability),
                price=VALUES(price),
                discount=VALUES(discount),
                mrp=VALUES(mrp);
        """
        try:
            with connection.cursor() as cursor:
                result = cursor.execute(insert_query, (
                    item['comp'],
                    item['fk_id'],
                    item['pincode'],
                    item['url'],
                    item['name'],
                    item['availability'],
                    item['price'],
                    item['discount'],
                    item['mrp'],
                    item['slug']
                ))
            connection.commit()
            if result == 0:
                logger.info(f"No new records inserted for URL: {url}")
            else:
                logger.info(f"Inserted/Updated record for URL: {url}")

            update_query = """
                UPDATE input_zepto_master_file SET status = %s WHERE slug = %s
            """
            try:
                with connection.cursor() as cursor:
                    cursor.execute(update_query, ("Done", slug))
                connection.commit()
                logger.info(f"Processed link No {id} with updated Status Done")
            except pymysql.MySQLError as e:
                logger.error(f"Database error during status update to Done: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during status update to Done: {e}")
        except pymysql.MySQLError as e:
            logger.error(f"Database error during insertion: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during insertion: {e}")

# Main scraping logic
try:
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM input_zepto_master_file WHERE status != 'Done'")
        results = cursor.fetchall()

    for links in results:
        cookies = {
            'storeName': str(links[7]),
            'store_id': str(links[6]),
            'cityName': str(links[5]),
            'latitude': str(links[8]),
            'longitude': str(links[9])
        }

        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

        id = str(links[0])
        pid = str(links[1])
        url = str(links[2])
        slug = str(links[10])
        pincode = str(links[4])

        try:
            reqq = fetch_with_retries(
                url,
                cookies=cookies,
                headers=headers,
                impersonate=random.choice(browser_list)
            )
            status_code = reqq.status_code
            response = TextResponse(url=reqq.url, body=reqq.text, encoding='utf-8')
            scraper(url, pincode, pid, response, id, status_code, page_save_pdp, slug)
        except Exception as e:
            logger.error(f"Failed to process URL {url} with error: {e}")

finally:
    connection.close()

