import json
import random
import re

import QC.db_config as db
import scrapy
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem


class BlkUpdatedSpider(scrapy.Spider):
    name = "blk_updated"
    allowed_domains = ["blinkit.com"]
    start_urls = ["https://blinkit.com"]

    def __init__(self, start_id, end_id):
        super().__init__()
        self.start_id = start_id
        self.end_id = end_id

        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
        self.cursor = self.con.cursor()

    def start_requests(self):
        # self.cursor.execute(
        #     f"select index_id, fkg_pid, link, SIZE_BLINKIT, UOM_BLK, Combo_Value_BLINKIT, pincode from mapped_blk_input where status='Pending' and index_id between {self.start_id} and {self.end_id}")
        self.cursor.execute(
            f"select index_id, fkg_pid, link, SIZE_BLINKIT, UOM_BLK, Combo_Value_BLINKIT, pincode from mapped_blk_input where index_id = 1120")
        results = self.cursor.fetchall()

        cookies_file = json.loads(
            open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\blinkit_cookies.json', 'r').read())

        for result in results:
            id = result[0]
            fkg_pid = result[1]
            blk_url = result[2]
            size = result[3]
            uom = result[4]
            combo_value = result[5]
            pincode = result[6]

            meta = {
                'fkg_pid': fkg_pid,
                'blk_url': blk_url,
                'size': size,
                'uom': uom,
                'combo_value': combo_value,
                'pincode': pincode,
                'input_id': id,
                'proxy': "http://scraperapi:de51e4aafe704395654a32ba0a14494d@proxy-server.scraperapi.com:8001"
            }

            browsers = [
                "chrome110",
                "edge99",
                "safari15_5"
            ]
            meta["impersonate"] = random.choice(browsers)

            cookies_from_file = cookies_file[pincode]

            if not cookies_from_file:
                print('Cookies not present')
                empty_item = dict()  # Create an instance of QcItem
                empty_item["comp"] = "BlinkIt"  # Set the company name
                empty_item['pincode'] = pincode  # Set the pincode
                empty_item['fk_id'] = fkg_pid  # Set the FK ID
                empty_item['url'] = blk_url  # Set the URL
                empty_item['name'] = ''  # Set the product name
                empty_item['mrp'] = ''  # Default empty value for MRP
                empty_item['discount'] = ''  # Default empty value for discount
                empty_item['availability'] = ''  # Default empty value for availability
                empty_item['price'] = ''  # Default empty value for price

                try:
                    field_list = []
                    value_list = []

                    for field in empty_item:
                        field_list.append(str(field))
                        value_list.append('%s')

                    fields = ','.join(field_list)
                    values = ", ".join(value_list)
                    insert_db = f"insert ignore into {db.db_data_table}( " + fields + " ) values ( " + values + " )"
                    self.cursor.execute(insert_db, tuple(empty_item.values()))
                    self.con.commit()
                    print('Data Inserted...')

                    update_query = f"""UPDATE mapped_blk_input SET status='unserviceable' WHERE index_id={id}"""
                    self.cursor.execute(update_query)
                    self.con.commit()
                    print('Status Updated')
                except Exception as e:
                    print(e)
            else:
                cookies = {
                    'gr_1_lat': str(cookies_from_file['latitude']),
                    'gr_1_lon': str(cookies_from_file['longitude']),
                }

                headers = {
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'no-cache',
                    # 'cookie': 'gr_1_deviceId=199feadd-c704-4277-a25b-ad84c3ee90af; __cfruid=686e7562471a6c9a38a46832f7e51ac74f23a9e8-1724301431; _cfuvid=uNRxn1_j3IxnvMFtkKASh3qaimqsfxLQqYyy_CrTAW0-1724301431376-0.0.1.1-604800000; _gcl_au=1.1.1548627087.1724301434; _gid=GA1.2.779382043.1724301435; _fbp=fb.1.1724301435236.649197619322819253; __cf_bm=wFXkry9xmyhynyVD_QELh.Nh5xRgrnmNP779FuYB1bM-1724303267-1.0.1.1-kJXUOOZqV8XVeOjbWz4JzOssYIm9h.kDcDTcvR6h0v1qGiqBzvuOFR.9MKk19.37i5RAGIyuZ8fC5g8wnEhvlA; _ga=GA1.1.1742480703.1724301435; _ga_JSMJG966C7=GS1.1.1724301434.1.1.1724303326.2.0.0; _ga_DDJ0134H6Z=GS1.2.1724301435.1.1.1724303327.3.0.0; gr_1_lat=28.6217627; gr_1_lon=77.0558233; gr_1_locality=Delhi%20Division; gr_1_landmark=New%20Delhi%2C%20Delhi%2C%20110059%2C%20India',
                    'pragma': 'no-cache',
                    'priority': 'u=0, i',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }

                yield scrapy.Request(
                    url=blk_url,
                    headers=headers,
                    cookies=cookies,
                    meta=meta,
                    dont_filter=True
                )

    def clean_json(self, raw_data):
        raw_data = raw_data.replace('window.grofers = {};', '')
        raw_data = raw_data.replace('window.grofers.PRELOADED_STATE = ', '')
        start_idx = raw_data.find('window.grofers.CONFIG = {')
        result = raw_data[:start_idx].strip()
        # with open('file.txt', 'w') as file:
        #     file.write(result.strip(';'))
        return json.loads(result.strip(';'))

    """
    extract_numeric_value function is used to extract numeric value
    e.g.: json contain unit like 10 ml and 10 GM
    so this function return 10 and 10.
    """

    def extract_numeric_value(self, product_size):
        match = re.match(r'(\d+)', product_size)
        return match.group(1) if match else None

    """
    store_logs funtion is used for maintain logs in database
    """

    def store_logs(self, input_id, status):
        self.cursor.execute("UPDATE mapped_blk_input SET status=%s where index_id=%s", (status, input_id))
        self.con.commit()

    def parse(self, response, **kwargs):

        meta = response.meta

        try:
            # Initialize an instance of QcItem to store product information
            item = QcItem()

            item['pincode'] = meta['pincode']
            item['comp'] = "BlinkIt"
            item['fk_id'] = meta['fkg_pid']
            item['url'] = meta['blk_url']

            # Extract raw JSON data embedded within the script tag containing 'window.grofers'
            raw_data = response.xpath("//script[contains(text(),'window.grofers')]/text()").get()

            # Clean and parse the raw JSON data into a Python dictionary
            json_data = self.clean_json(raw_data)

            # Navigate to the product details in the parsed JSON
            product_data = json_data.get('ui', {}).get('pdp', {}).get('product', {}).get('details', {})

            # Initialize product item fields with empty values
            item['name'] = ''
            item['price'] = ''
            item['mrp'] = ''
            item['discount'] = ''
            item['availability'] = ''

            # Extract product variant information from product details
            variants = product_data.get('variantsInfo', '')

            # Check if there are variants available
            if variants:
                for variant in variants:
                    # Extract the numeric part of the size/unit from the variant information
                    size_and_uom = variant['unit'].split('x')
                    combo = None
                    size = None

                    combo_status = True

                    if '(' in variant['unit'] and ')' in variant['unit']:
                        item['availability'] = variant.get('inventory', 0) > 0
                        # Set availability status based on inventory
                        item['availability'] = variant.get('inventory', 0) > 0
                        item['name'] = variant.get('name', '')  # Set product name

                        # If the product is available, populate price, MRP, and discount details
                        if item['availability']:
                            item['price'] = variant.get('price', '')
                            item['mrp'] = variant.get('mrp', '')
                            item['discount'] = variant.get('offer', '').replace(' OFF', '')
                        break

                    if len(size_and_uom) > 1:
                        combo = float(size_and_uom[0].strip())
                        size = self.extract_numeric_value(size_and_uom[1].strip())

                        if 'combo_value' in meta and meta['combo_value'] and (meta['combo_value']) != 1.0:
                            # If it exists, compare with combo
                            combo_status = combo == meta['combo_value']
                        else:
                            # If 'combo_value' does not exist, store True
                            combo_status = True
                    else:
                        size = self.extract_numeric_value(variant['unit'])

                    # Validate the extracted unit against the expected size from database
                    if size == meta['size'] and combo_status:
                        # Set availability status based on inventory
                        item['availability'] = variant.get('inventory', 0) > 0
                        item['name'] = variant.get('name', '')  # Set product name

                        # If the product is available, populate price, MRP, and discount details
                        if item['availability']:
                            item['price'] = variant.get('price', '')
                            item['mrp'] = variant.get('mrp', '')
                            item['discount'] = variant.get('offer', '').replace(' OFF', '')
                        break  # Exit loop once the matching variant is found

            # Log success message after yielding the item
            self.store_logs(meta['input_id'], 'Success')
            yield item

        except Exception as e:
            # In case of an error, print the error message and log the error
            print("ERROR: ", e)
            self.store_logs(meta['input_id'], 'Error')

    if __name__ == '__main__':
        execute(f"scrapy crawl blk_updated -a start_id=10001 -a end_id=20000".split())
