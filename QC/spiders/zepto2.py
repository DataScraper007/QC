# import pandas as pd
# from curl_cffi import requests
# import pymysql
# from scrapy import Selector
# from datetime import datetime
# from scrapy.http import TextResponse
#
# from parsel import Selector
#
# df_store_ids = pd.read_csv(r"D:\myProjects\2024\july\QC_SCRAPER\QC\QC\input\zepto_store_ids.csv", index_col=False)
#
# req_count = 0
#
# delivery_date = datetime.today().strftime("%Y%m%d")
#
#
# host = 'localhost'
# user = 'root'
# password = 'actowiz'
# database = 'qcg'
#
#
# try:
#     connection = pymysql.connect(host=host, user=user, password=password, database=database)
#
#     with connection.cursor() as cursor:
#
#         create_table = f"""
#         CREATE TABLE IF NOT EXISTS `zepto_{delivery_date}` (
#             `Id` INT NOT NULL AUTO_INCREMENT,
#             `comp` VARCHAR(255) DEFAULT 'N/A',
#             `fk_id` VARCHAR(255) DEFAULT 'N/A',
#             `pincode` VARCHAR(255) DEFAULT 'N/A',
#             `url` VARCHAR(255) DEFAULT 'N/A',
#             `name` VARCHAR(255) DEFAULT 'N/A',
#             `availability` VARCHAR(255) DEFAULT 'N/A',
#             `price` VARCHAR(255) DEFAULT 'N/A',
#             `discount` VARCHAR(255) DEFAULT 'N/A',
#             `mrp` VARCHAR(255) DEFAULT 'N/A',
#             PRIMARY KEY (`Id`),
#             UNIQUE KEY `fid` (`fk_id`, `pincode`)
#         ) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4;
#         """
#         cursor.execute(create_table)
#
#
#         def scraper(url, pincode, pid, response, countt, statuss):
#
#             print(f"Status Code: {statuss}")
#
#             print(countt)
#
#             item = {}
#
#             item['url'] = url
#             item['pincode'] = pincode
#             item['comp'] = "Zepto"
#             item['fk_id'] = pid
#
#             try:
#                 item['name'] = response.xpath("//h1/text()").get()
#             except:
#                 item['name'] = None
#
#             try:
#                 item['mrp'] = response.xpath("//*[@data-testid='pdp-discounted-price']/text()").get().strip("₹")
#             except:
#                 item['mrp'] = None
#
#             try:
#                 item['price'] = response.xpath("//*[@data-test-id='pdp-selling-price']/text()").get().strip("₹")
#             except:
#                 item['price'] = None
#
#             try:
#                 item['discount'] = response.xpath("//div[contains(text(),'% Off')]/text()").get().strip("% Off")
#             except:
#                 item['discount'] = None
#
#             item['availability'] = False if 'Out of Stock' in response.text else True
#             if not item['name']:
#                 item['availability'] = False
#
#             insert_query = f"""
#             INSERT INTO `zepto_{delivery_date}` (comp, fk_id, pincode, url, name, availability, price, discount, mrp)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON DUPLICATE KEY UPDATE
#                 name=VALUES(name),
#                 availability=VALUES(availability),
#                 price=VALUES(price),
#                 discount=VALUES(discount),
#                 mrp=VALUES(mrp);
#             """
#
#             try:
#                 with connection.cursor() as cursor:
#                     cursor.execute(insert_query, (
#                         item['comp'],
#                         item['fk_id'],
#                         item['pincode'],
#                         item['url'],
#                         item['name'],
#                         item['availability'],
#                         item['price'],
#                         item['discount'],
#                         item['mrp']
#                     ))
#                 connection.commit()
#             except Exception as e:
#                 print(f"Error inserting data into the database: {e}")
#
#
#         # Fetch links from the database
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT * FROM zepto_links_pids")
#             results = cursor.fetchall()
#
#             total_extraction = int(len(df_store_ids["Pincode"]))*len(list(results))
#
#         # Scrape each store ID
#         for index, row in df_store_ids.iterrows():
#             for links in results[3000:]:
#                 cookies = {
#                     'store_id': str(row["Store_ID"]),
#                     'latitude': str(row["Lat"]),
#                     'longitude': str(row["Long"])
#                 }
#
#                 headers = {
#                     'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#                     'accept-language': 'en-US,en;q=0.9',
#                     'sec-ch-ua-platform': '"Windows"',
#                     'sec-fetch-dest': 'document',
#                     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36'
#                 }
#
#                 url = str(links[1])
#                 pid = str(links[0])
#
#                 reqq = requests.get(
#                     url,
#                     cookies=cookies,
#                     headers=headers,
#                 )
#
#                 statuss = reqq.status_code
#
#                 pincode = str(row["Pincode"])
#
#                 # print(reqq.text)
#
#                 # response = Selector(text=reqq.text)
#
#                 # print(type(reqq.text))
#
#                 response = TextResponse(url=reqq.url, body=reqq.text, encoding='utf-8')
#
#                 req_count += 1
#
#                 countt = f"{req_count}/{total_extraction}"
#
#                 scraper(url, pincode, pid, response, countt, statuss)
#
# except pymysql.MySQLError as e:
#     print(f"Error connecting to the database: {e}")
# finally:
#     connection.close()
