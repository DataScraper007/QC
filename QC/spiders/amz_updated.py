import scrapy
import json
import os
import pymysql
from scrapy.cmdline import execute
from QC.items import QcItem
import QC.db_config as db


class AmzUpdatedSpider(scrapy.Spider):
    name = "amz_updated"
    allowed_domains = ["amazon.in"]
    start_urls = ["https://amazon.in"]

    def __init__(self, pincode, **kwargs):
        super().__init__(**kwargs)
        self.pincode = pincode

        # Load cookies from JSON file
        with open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\amazon_cookies.json', 'r') as f:
            self.cookies = json.load(f).get(str(pincode), {})

        # Database connection setup
        self.con = pymysql.connect(
            host=db.db_host,
            user=db.db_user,
            password=db.db_password,
            database=db.db_name
        )
        self.cursor = self.con.cursor()

        # Page save path
        self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save\{db.delivery_date}\amz\HTMLS'
        os.makedirs(self.page_save_pdp, exist_ok=True)

    def start_requests(self):
        query = (
            "SELECT fkg_pid, amz_url "
            "FROM input_amz "
            "WHERE amz_url != 'NA' "
            "AND fkg_pid IN (SELECT pid FROM pid_pincode WHERE pincode=%s)"
        )
        self.cursor.execute(query, (self.pincode,))
        results = self.cursor.fetchall()

        for fkg_pid, amz_url in results:
            amz_url = amz_url.replace('?fpw', '?s=nowstore').strip('=alm')
            meta = {
                'fkg_pid': fkg_pid,
                'amz_url': amz_url,
                'proxy': "https://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
            }
            headers = {
                'accept-language': 'en-US,en;q=0.9',
                'user_agent': 'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/16.0 Mobile Safari/534.30',
            }
            yield scrapy.Request(
                url= amz_url,
                headers=headers,
                cookies=self.cookies,
                meta=meta
            )



    def parse(self, response, **kwargs):
        item = QcItem()

        # Common item fields
        item["comp"] = "AmazonFresh"
        item['pincode'] = self.pincode
        item['fk_id'] = response.meta['fkg_pid']
        item['url'] = response.meta['amz_url']

        # Save page content
        page_save_path = os.path.join(self.page_save_pdp, f"{item['fk_id']}{self.pincode}.html")
        with open(page_save_path, "w", encoding="utf-8") as f:
            f.write(response.text)

        store_availability_status = response.xpath(
            '//span[contains(text(), "This store is not available")]/text()').get()

        if store_availability_status:
            item.update({
                'name': '',
                'mrp': '',
                'price': '',
                'discount': '',
                'availability': ''
            })
            yield item
            return

        # Extract product details
        product_name = response.xpath("//meta[@name='title']/@content").get() or \
                       response.xpath("//*[@id='title']/text()").get()
        if product_name:
            product_name = product_name.split(" : Amazon.in: ")[0]
            product_name = product_name.replace('Amazon.com:', '').strip()

        item['name'] = product_name.replace(' - Amazon.in', '') if product_name else ''

        mrp = response.xpath(
            "//span[contains(text(),'MRP:')]/following-sibling::span/span/text() | "
            "//td[contains(text(),'M.R.P.:')]/following-sibling::td/span/span/text() | "
            "//span[contains(text(),'M.R.P.:') and not(ancestor::span[@id='subscriptionPrice']) and ancestor::div[@data-feature-name='corePriceDisplay_desktop']]//span/text() | "
            "//td[contains(text(),'Bundle List Price')]/following-sibling::td/span/span[@class='a-offscreen']/text()"
        ).get()

        item['mrp'] = mrp.strip('₹').replace(',', '') if mrp else ''

        # try:
        #     price = response.xpath(
        #         "//span[contains(@class,'buyingPrice')]/span/text() |  //td[contains(text(),'Deal') or contains(text(),'Price')]/following-sibling::td//span[@data-a-color='price']//span/text() | //span[contains(@class,'priceToPay')]/span/text()").get('').strip()
        # except:
        #     price = ''
        # if price == '':
        #     price = response.xpath(
        #         "//span[contains(@class,'priceToPay')]//span[@class='a-price-whole']//text()").get()
        #     if price == None:
        #         price = response.xpath('//span[@id="priceblock_ourprice"]/span/text()').get('')
        #
        # item['price'] = (price.strip('₹')).replace(',', '')
        # Define the list of XPath expressions to try
        xpaths = [
            "//span[contains(@class,'buyingPrice')]/span/text()",
            "//td[contains(text(),'Deal') or contains(text(),'Price')]/following-sibling::td//span[@data-a-color='price']//span/text()",
            "//span[contains(@class,'priceToPay')]/span/text()",
            "//span[contains(@class,'priceToPay')]//span[@class='a-price-whole']//text()",
            '//span[@id="priceblock_ourprice"]/span/text()'
        ]
        price = ''
        # Try each XPath expression until a non-empty value is found
        for xpath in xpaths:
            price = response.xpath(xpath).get(default='').strip()
            if price:
                break

        # Clean the price value
        item['price'] = price.replace('₹', '').replace(',', '') if price else ''


        discount = response.xpath(
            "//div[contains(@class,'discount-sticker')]/p//text() | "
            "//span[not(ancestor::span[@id='subscriptionPrice']) and ancestor::div[@data-feature-name='corePriceDisplay_desktop'] and contains(@class, 'savingsPercentage')]/text() | "
            "//td[contains(@class,'a-color-price')]//span[contains(@class,'a-color-price') ]/text()"
        ).getall()
        discount = next((d.strip() for d in discount if '%' in d), '')
        item['discount'] = discount.strip('-') if discount and discount not in {'0%', '0'} else ''

        #TODO : check availability
        raw_stock = response.xpath("//div[contains(@id,'availability')]/span/text()").get()
        if raw_stock == None or raw_stock.strip() == '':
            raw_stock = response.xpath(
                                "//div[contains(@id,'buybox')]//*[contains(text(),'Buy Now')]/text()").get()
            if raw_stock:
                item['availability'] = True
            else:
                item['availability'] = False

            currently_unavailable = response.xpath("//span[contains(text(), 'Currently unavailable')]/text()").get()
            if currently_unavailable:
                item['availability'] = False
        # print(item)
        yield item


if __name__ == '__main__':
    execute(f"scrapy crawl amz_updated -a pincode=431001".split())

'''old code'''

# import scrapy
# import base64
# import json
# import os
# import QC.db_config as db
# import pymysql
# from scrapy.cmdline import execute
# from QC.items import QcItem, QcItem_amz
#
#
# class AmzUpdatedSpider(scrapy.Spider):
#     name = "amz_updated"
#     allowed_domains = ["amazon.in"]
#     start_urls = ["https://amazon.in"]
#
#     def __init__(self, pincode, **kwargs):
#         super().__init__(**kwargs)
#         self.pincode = pincode
#
#         self.cookies = \
#             json.loads(open(r'C:\Users\Admin\PycharmProjects\QC\QC\cookies\amazon_cookies.json', 'r').read())[
#                 str(pincode)]
#         self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, database=db.db_name)
#         self.cursor = self.con.cursor()
#         self.page_save_pdp = fr'C:\Users\Admin\PycharmProjects\page_save\{db.delivery_date}\amz\HTMLS'
#
#     def start_requests(self):
#         self.cursor.execute(
#             f"select fkg_pid, amz_url from input_amz where amz_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='{self.pincode}')")
#         # f"select fkg_pid, amz_url from input where amz_url != 'NA' and fkg_pid in (select pid from pid_pincode where pincode='781011') and FKG_PID = 'CAFFZ448T8G9GVRS'")
#         results = self.cursor.fetchall()
#
#         for result in results:
#             fkg_pid = result[0]
#             amz_url = result[1].replace('?fpw', '?s=nowstore')
#             amz_url = "https://www.amazon.in/Society-Tea-500g-Jar/dp/B079H3CWQX?s=nowstore"
#             fkg_pid = "TEAFG2B2GVYVQZS9"
#
#             meta = {
#                 'fkg_pid': fkg_pid,
#                 'amz_url': amz_url,
#                 'proxy': "https://scraperapi:64a773e99ca0093e4f80e217a71f821b@proxy-server.scraperapi.com:8001"
#             }
#             headers = {
#                 'accept-language': 'en-US,en;q=0.9',
#                 'user_agent': 'Mozilla/5.0 (Linux; U; Android 4.3; en-us; SM-N900T Build/JSS15J) AppleWebKit/534.30 (KHTML, like Gecko) Version/16.0 Mobile Safari/534.30',
#             }
#             yield scrapy.Request(
#                 url=amz_url,
#                 headers=headers,
#                 cookies=self.cookies,
#                 meta=meta
#             )
#             break
#
#     def parse(self, response, **kwargs):
#
#         item = QcItem()
#
#         # item = QcItem_amz()
#
#         item["comp"] = "AmazonFresh"
#         meta = response.meta
#         item['pincode'] = self.pincode
#         item['fk_id'] = meta['fkg_pid']
#         item['url'] = meta['amz_url']
#         # item['page_save_path'] = self.page_save_pdp
#         # item['page_save_id'] = meta['fkg_pid'] + self.pincode + ".html"
#         store_availability_status = response.xpath(
#             '//span[contains(text(), "This store is not available")]/text()').get()
#         if not os.path.exists(self.page_save_pdp):
#             os.makedirs(self.page_save_pdp)
#         # hash_id = hashlib.sha256(page_name.encode()).hexdigest()
#         open(self.page_save_pdp + meta['fkg_pid'] + self.pincode + ".html", "w", encoding="utf-8").write(
#             response.text)
#
#         if store_availability_status is not None:
#             item['name'] = ''
#             item['mrp'] = ''
#             item['price'] = ''
#             item['discount'] = ''
#             item['availability'] = ''
#
#             yield item
#
#         else:
#             product_name = response.xpath("//meta[@name='title']/@content").get()
#             if product_name == None:
#                 product_name = response.xpath("//*[@id='title']/text()").get()
#             try:
#                 product_name = product_name.split(" : Amazon.in: ")[0] if product_name else ''
#
#                 if "Amazon.com:" in product_name:
#                     product_name = product_name.strip("Amazon.com:")
#             except:
#                 product_name = ''
#             print(product_name)
#             item['name'] = product_name.replace(' - Amazon.in', '')
#             mrp = response.xpath(
#                 "//span[contains(text(),'MRP:')]/following-sibling::span/span/text() | //td[contains(text(),'M.R.P.:')]/following-sibling::td/span/span/text() | //span[contains(text(),'M.R.P.:')]//span/text() | //td[contains(text(),'Bundle List Price')]/following-sibling::td/span/span[@class='a-offscreen']/text()").get()
#
#             item['mrp'] = (mrp.strip('₹')).replace(',', '') if mrp is not None else ''
#
#             try:
#                 # price = response.xpath(
#                 #     "//span[contains(@class,'buyingPrice')]/span/text() | //td[contains(text(),'Deal') or contains(text(),'Price')]/following-sibling::td/span/span/text() | //span[contains(@class,'priceToPay')]/span/text()").get(
#                 #     '').strip()
#                 price = response.xpath(
#                     "//span[contains(@class,'buyingPrice')]/span/text() |  //td[contains(text(),'Deal') or contains(text(),'Price')]/following-sibling::td//span[@data-a-color='price']//span/text() | //span[contains(@class,'priceToPay')]/span/text()").get(
#                     '').strip()
#             except:
#                 price = ''
#             if price == '':
#                 price = response.xpath(
#                     "//span[contains(@class,'priceToPay')]//span[@class='a-price-whole']//text()").get()
#                 # price = "".join(price).strip()
#                 if price == None:
#                     price = response.xpath('//span[@id="priceblock_ourprice"]/span/text()').get('')
#                     # price = "".join(price).strip()
#
#             item['price'] = (price.strip('₹')).replace(',', '')
#
#             raw_discount = response.xpath("//div[contains(@class,'discount-sticker')]/p//text()").getall()
#             discount = None
#
#             if raw_discount:
#                 for i in raw_discount:
#                     if '%' in i:
#                         discount = i.strip()
#                         break
#             if not discount:
#                 # discount = response.xpath("//span[contains(@class,'savingsPercentage')]//text()").get()
#                 discount = response.xpath(
#                     "//span[not(ancestor::span[@id='subscriptionPrice'])and ancestor::div[@data-feature-name='corePriceDisplay_desktop'] and contains(@class, 'savingsPercentage')]/text()").get()
#
#             if not discount:
#                 raw_discount = response.xpath(
#                     "//td[contains(@class,'a-color-price')]//span[contains(@class,'a-color-price') ]/text()").getall()
#                 # if not raw_discount:
#                 #     raw_discount = response.xpath("//*[contains(@class,'savingsPercentage')]/text()").getall()
#                 for dis in raw_discount:
#                     if '%' in dis:
#                         discount = dis.strip().replace('(', '').replace(')', '')
#                         break
#             if discount == '0%' or discount == '0':
#                 discount = ''
#             item['discount'] = discount.strip('-') if discount is not None else ''
#
#             raw_stock = response.xpath("//div[contains(@id,'availability')]/span/text()").get()
#             try:
#                 if raw_stock.strip() != '':
#                     if 'in stock' or 'स्टॉक में है' in raw_stock.lower().strip() or raw_stock.lower().strip() == 'in stock' or 'left in stock' in raw_stock.lower().strip():
#                         item['availability'] = True
#                     else:
#                         item['availability'] = False
#             except:
#                 item['availability'] = False
#
#             if raw_stock == None or raw_stock.strip() == '':
#                 raw_stock = response.xpath(
#                     "//div[contains(@id,'buybox')]//*[contains(text(),'Buy Now')]/text()").get()
#                 if raw_stock:
#                     item['availability'] = True
#                 else:
#                     item['availability'] = False
#
#             print(item)
#
#     if __name__ == '__main__':
#         execute(f"scrapy crawl amz_updated -a pincode=431001".split())
