import scrapy


class QcItem(scrapy.Item):
    index_id = scrapy.Field()
    comp = scrapy.Field()
    fk_id = scrapy.Field()
    pincode = scrapy.Field()
    discount = scrapy.Field()
    mrp = scrapy.Field()
    price = scrapy.Field()
    name = scrapy.Field()
    availability = scrapy.Field()
    url = scrapy.Field()

class QcItem_amz(scrapy.Item):
    comp = scrapy.Field()
    fk_id = scrapy.Field()
    pincode = scrapy.Field()
    url = scrapy.Field()
    page_save_path = scrapy.Field()
    page_save_id = scrapy.Field()


