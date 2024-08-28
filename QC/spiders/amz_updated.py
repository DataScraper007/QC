import scrapy
import json
import os
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem
import QC.db_config as db


class AmzUpdatedSpider(scrapy.Spider):
    name = "amz"

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)

        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name,
                                   autocommit=True)
        self.cursor = self.con.cursor()

        # self.page_save_pdp = f'C:/pankaj_page_save/{db.delivery_date}/amz/HTMLS/'
        self.start_id = start_id
        self.end_id = end_id

        self.input_table = 'mapped_amazon_input'

    def start_requests(self):
        self.cursor.execute(
            f"select index_id, pincode, fkg_pid, link from {self.input_table} where status='Pending' and index_id between {self.start_id} and {self.end_id}")

        results = self.cursor.fetchall()
        print('Fetched', len(results))

        for result in results:
            index_id = result[0]
            pincode = result[1]
            fkg_pid = result[2]
            amz_url = result[3].replace('?fpw', '?s=nowstore').replace('?th', '?s=nowstore')
            if '?' not in amz_url:
                amz_url = amz_url + '?s=nowstore'

            cookies = \
                json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\amazon_cookies.json', 'r').read())[
                    pincode]

            if not cookies:
                empty_item = dict()  # Create an instance of QcItem
                empty_item["comp"] = "AmazonFresh"  # Set the company name
                empty_item['pincode'] = pincode  # Set the pincode
                empty_item['fk_id'] = fkg_pid  # Set the FK ID
                empty_item['url'] = amz_url  # Set the URL
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

                    update_query = f"""UPDATE {self.input_table} SET status='unserviceable' WHERE index_id={index_id}"""
                    self.cursor.execute(update_query)
                    self.con.commit()
                    print('Status Updated')
                except Exception as e:
                    print(e)
            else:
                meta = {
                    'fkg_pid': fkg_pid,
                    'amz_url': amz_url,
                    'pincode': pincode,
                    'index_id': index_id,
                    'proxy': "https://scraperapi:197b5b148bb253822558090a5bbf850d@proxy-server.scraperapi.com:8001"
                }
                headers = {
                    'accept-language': 'en-US,en;q=0.9',
                    'user_agent': 'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/16.0 Mobile Safari/534.30',
                }
                yield scrapy.Request(
                    url=amz_url,
                    headers=headers,
                    cookies=cookies,
                    meta=meta,
                    dont_filter=True
                )

    def parse(self, response, **kwargs):
        item = QcItem()

        # Common item fields
        item["comp"] = "AmazonFresh"
        item['index_id'] = response.meta['index_id']
        item['pincode'] = response.meta['pincode']
        item['fk_id'] = response.meta['fkg_pid']
        item['url'] = response.meta['amz_url']

        # # Save page content
        # page_save_path = os.path.join(self.page_save_pdp, f"{item['fk_id']}{response.meta['pincode']}.html")
        # with open(page_save_path, "w", encoding="utf-8") as f:
        #     f.write(response.text)

        # Store availability check
        store_availability_status = response.xpath(
            '//span[contains(text(), "This store is not available")]/text()'
        ).get()

        if store_availability_status:
            print("store not available")
            yield item
            return

        # Extract product details
        product_name = response.xpath("//h1[@id='title']/span[@id='productTitle']/text()").get('')
        item['name'] = product_name.strip()

        mrp = response.xpath(
            "//span[contains(text(),'MRP:')]/following-sibling::span/span/text() | "
            "//td[contains(text(),'M.R.P.:')]/following-sibling::td/span/span/text() | "
            "//span[contains(text(),'M.R.P.:') and not(ancestor::span[@id='subscriptionPrice']) and ancestor::div[@data-feature-name='corePriceDisplay_desktop']]//span/text() | "
            "//td[contains(text(),'Bundle List Price')]/following-sibling::td/span/span[@class='a-offscreen']/text()"
        ).get('')

        item['mrp'] = mrp.strip('₹').replace(',', '')

        # Combined XPath to extract price using 'or' logic
        price_xpath = (
            "//span[@id='priceblock_ourprice']/span/text() | "
            "//div[@id='corePriceDisplay_desktop_feature_div' or @id='corePrice_feature_div']//span[@class='a-price-whole']/text() |"
            "//td[contains(text(),'Combo Price:')]/following-sibling::td//span/text()"
            # "//span[contains(@class,'buyingPrice')]/span/text() | "
            # "//span[contains(@class,'priceToPay')]/span/text()"
        )
        price = response.xpath(price_xpath).get('').strip()

        # Clean the price value
        item['price'] = price.replace('₹', '').replace(',', '')

        discount = response.xpath(
            "//div[contains(@class,'discount-sticker')]/p//text() | "
            "//span[not(ancestor::span[@id='subscriptionPrice']) and ancestor::div[@data-feature-name='corePriceDisplay_desktop'] and contains(@class, 'savingsPercentage')]/text() | "
            "//td[contains(@class,'a-color-price')]//span[contains(@class,'a-color-price') ]/text()"
        ).getall()

        discount = next((d.strip() for d in discount if '%' in d), '')
        item['discount'] = discount.replace('(', '').replace(')', '').strip('-') if discount and discount not in {'0%',
                                                                                                                  '0'} else ''

        # Check Availability
        currently_unavailable = response.xpath("//span[contains(text(), 'Currently unavailable')]/text()").get()
        if currently_unavailable:
            item['availability'] = False
        else:
            stock_status = response.xpath("//div[contains(@id,'availability')]/span/text()").get()
            if stock_status and stock_status.strip().lower() == 'in stock':
                item['availability'] = True
            else:
                buy_now_status = response.xpath(
                    "//div[contains(@id,'buybox')]//*[contains(text(),'Buy Now')]/text()").get()
                item['availability'] = bool(buy_now_status)
        yield item


if __name__ == '__main__':
    execute(f"scrapy crawl amz -a start_id=1001 -a end_id=5000".split())
