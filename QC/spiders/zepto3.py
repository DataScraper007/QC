# import pandas as pd
# from curl_cffi import requests
# import pymysql
# from scrapy import Selector
# from datetime import datetime
# from scrapy.http import TextResponse
# from concurrent.futures import ThreadPoolExecutor, as_completed
# # from threading import Lock
#
# df_store_ids = pd.read_csv(r"D:\myProjects\2024\july\QC_SCRAPER\QC\QC\input\zepto_store_ids.csv", index_col=False)
#
# delivery_date = datetime.today().strftime("%Y%m%d")
#
# host = 'localhost'
# user = 'root'
# password = 'actowiz'
# database = 'qcg'
#
# # Create a lock object for thread-safe database operations
# # db_lock = Lock()
#
# def create_table(cursor):
#     create_table_query = f"""
#     CREATE TABLE IF NOT EXISTS `zepto_{delivery_date}` (
#         `Id` INT NOT NULL AUTO_INCREMENT,
#         `comp` VARCHAR(255) DEFAULT 'N/A',
#         `fk_id` VARCHAR(255) DEFAULT 'N/A',
#         `pincode` VARCHAR(255) DEFAULT 'N/A',
#         `url` VARCHAR(255) DEFAULT 'N/A',
#         `name` VARCHAR(255) DEFAULT 'N/A',
#         `availability` VARCHAR(255) DEFAULT 'N/A',
#         `price` VARCHAR(255) DEFAULT 'N/A',
#         `discount` VARCHAR(255) DEFAULT 'N/A',
#         `mrp` VARCHAR(255) DEFAULT 'N/A',
#         PRIMARY KEY (`Id`),
#         UNIQUE KEY `fid` (`fk_id`, `pincode`)
#     ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
#     """
#     cursor.execute(create_table_query)
#
# def insert_data(connection, item):
#     insert_query = f"""
#     INSERT INTO `zepto_{delivery_date}` (comp, fk_id, pincode, url, name, availability, price, discount, mrp)
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ON DUPLICATE KEY UPDATE
#         name=VALUES(name),
#         availability=VALUES(availability),
#         price=VALUES(price),
#         discount=VALUES(discount),
#         mrp=VALUES(mrp);
#     """
#     try:
#         # with db_lock:
#         with connection.cursor() as cursor:
#             cursor.execute(insert_query, (
#                 item['comp'],
#                 item['fk_id'],
#                 item['pincode'],
#                 item['url'],
#                 item['name'],
#                 item['availability'],
#                 item['price'],
#                 item['discount'],
#                 item['mrp']
#             ))
#         connection.commit()
#     except Exception as e:
#         print(f"Error inserting data into the database: {e}")
#
# def scraper(url, pincode, pid, response, countt, statuss, connection):
#     print(f"Status Code: {statuss}")
#     print(countt)
#
#     item = {
#         'url': url,
#         'pincode': pincode,
#         'comp': "Zepto",
#         'fk_id': pid,
#         'name': response.xpath("//h1/text()").get() or None,
#         'mrp': response.xpath("//*[@data-testid='pdp-discounted-price']/text()").get().strip("₹") if response.xpath("//*[@data-testid='pdp-discounted-price']/text()") else None,
#         'price': response.xpath("//*[@data-test-id='pdp-selling-price']/text()").get().strip("₹") if response.xpath("//*[@data-test-id='pdp-selling-price']/text()") else None,
#         'discount': response.xpath("//div[contains(text(),'% Off')]/text()").get().strip("% Off") if response.xpath("//div[contains(text(),'% Off')]/text()") else None,
#         'availability': False if 'Out of Stock' in response.text or not response.xpath("//h1/text()").get() else True
#     }
#
#     insert_data(connection, item)
#
# def main():
#     try:
#         connection = pymysql.connect(host=host, user=user, password=password, database=database)
#         with connection.cursor() as cursor:
#             create_table(cursor)
#
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT * FROM input_zepto_links_pids")
#             results = cursor.fetchall()
#
#         total_extraction = len(df_store_ids["Pincode"]) * len(results)
#
#         with ThreadPoolExecutor(max_workers=5) as executor:
#             futures = []
#
#             for index, row in df_store_ids.iterrows():
#                 for links in results:
#                     cookies = {
#                         'store_id': str(row["Store_ID"]),
#                         'latitude': str(row["Lat"]),
#                         'longitude': str(row["Long"])
#                     }
#
#                     headers = {
#                         'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#                         'accept-language': 'en-US,en;q=0.9',
#                         'sec-ch-ua-platform': '"Windows"',
#                         'sec-fetch-dest': 'document',
#                         'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
#                     }
#
#                     url = str(links[1])
#                     pid = str(links[0])
#                     pincode = str(row["Pincode"])
#
#                     try:
#                         reqq = requests.get(url, cookies=cookies, headers=headers)
#                         statuss = reqq.status_code
#                         response = TextResponse(url=reqq.url, body=reqq.text, encoding='utf-8')
#
#                         countt = f"{len(futures) + 1}/{total_extraction}"
#                         futures.append(executor.submit(scraper, url, pincode, pid, response, countt, statuss, connection))
#                     except Exception as e:
#                         print(f"Error making request: {e}")
#
#             for future in as_completed(futures):
#                 try:
#                     future.result()
#                 except Exception as e:
#                     print(f"Thread error: {e}")
#
#     except pymysql.MySQLError as e:
#         print(f"Error connecting to the database: {e}")
#     finally:
#         connection.close()
#
# if __name__ == "__main__":
#     main()
