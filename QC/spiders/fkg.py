import html
import json
import os
import QC.db_config as db
import pymysql
import scrapy
from scrapy.cmdline import execute
from QC.items import QcItem


class FkgSpider(scrapy.Spider):
    name = "fkg"
    is_debug = False
    debug_pid = "ARDFA6WQ8GP9KSV8"

    def __init__(self, start_id, end_id):
        super().__init__()
        self.start_id = start_id
        self.end_id = end_id
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password)
        self.cursor = self.con.cursor()
        self.input_table = 'mapped_fkg_input'
        # self.page_save_pdp = f'D:/pankaj_page_save/{db.delivery_date}/fkg/HTMLS/'

    def start_requests(self):
        self.cursor.execute(
            f"select * from {self.input_table} where status='Pending' and index_id between {self.start_id} and {self.end_id}")
        results = self.cursor.fetchall()

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        }
        cookies_file = json.loads(
            open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\flipkart_cookies.json', 'r').read())

        for result in results:
            id = result[0]
            pid = result[2]
            url = f'https://www.flipkart.com/%20/p/%20?pid={pid}&marketplace=GROCERY'
            pincode = result[1]

            cookies = cookies_file[pincode]

            if not cookies:
                print('Cookies not present')
                empty_item = dict()  # Create an instance of QcItem
                empty_item["comp"] = "Flipkart Grocery"  # Set the company name
                empty_item['pincode'] = pincode  # Set the pincode
                empty_item['fk_id'] = pid  # Set the FK ID
                empty_item['url'] = url  # Set the URL
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

                    update_query = f"""UPDATE {self.input_table} SET status='unserviceable' WHERE index_id={id}"""
                    self.cursor.execute(update_query)
                    self.con.commit()
                    print('Status Updated')

                except Exception as e:
                    print(e)
            else:
                yield scrapy.Request(
                    url=url,
                    headers=headers,
                    cookies=cookies,
                    callback=self.parse,
                    meta={"id": id, "pid": pid, "pincode": pincode},
                    dont_filter=True
                )

    def parse(self, response, **kwargs):
        meta = response.meta

        # if not os.path.exists(self.page_save_pdp):
        #     os.makedirs(self.page_save_pdp)
        #     open(self.page_save_pdp + meta['pid'] + meta['pincode'] + ".html", "w", encoding="utf-8").write(
        #         response.text)

        item = QcItem()
        item['index_id'] = meta['id']
        item['comp'] = 'Flipkart Glocery'
        item['fk_id'] = meta['pid']
        item['pincode'] = meta['pincode']
        item['url'] = response.url

        # Extract product name
        name_data = response.xpath('//h1[@class="_6EBuvT"]/span/text()').getall()
        item['name'] = html.unescape(''.join(name_data))
        # Extract price information
        price_data = response.xpath('//div[@class="hl05eU"]')
        item['price'] = price_data.xpath(
            './div[@class="Nx9bqj CxhGGd"]/text()'
        ).get('').replace('₹', '').replace(',', '')

        # Extract MRP (Maximum Retail Price) and format it
        mrp_data = price_data.xpath('./div[contains(@class, "yRaY8j") and contains(@class, "A6+E6v")]/text()').getall()
        item['mrp'] = ''.join(mrp_data).replace('₹', '').replace(',', '')

        # Extract discount information and clean it up
        item['discount'] = price_data.xpath('./div[@class="UkUFwK WW8yVX"]/span/text()').get('').replace('off',
                                                                                         '').strip()

        # Check product availability based on specific text or button presence
        availability = response.xpath(
            '//div[contains(text(),"Sold Out") or contains(text(), "Currently Unavailable")]/text() | '
            '//button[contains(text(), "NOTIFY ME")]/text()'
        ).get()

        # Set availability status based on the extracted information
        item['availability'] = availability is None  # True if available, False if sold out

        yield item


if __name__ == '__main__':
    execute(f"scrapy crawl fkg -a start_id=0 -a end_id=10".split())
