# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from sqlite3 import IntegrityError

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from QC.items import QcItem, QcItem_amz
import QC.db_config as db


class QcPipeline:

    def open_spider(self, spider):
        db.db_data_table = f"{spider.name}_{db.delivery_date}"
        try:
            create_database = f"CREATE DATABASE IF NOT EXISTS `{db.db_name}`;"
            spider.cursor.execute(create_database)
            spider.cursor.execute(f"USE `{db.db_name}`;")
        except Exception as e:
            print(e)


        try:
            # Create the data table if not exists
            create_table = f"""CREATE TABLE IF NOT EXISTS `{db.db_data_table}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                        `comp` VARCHAR (255) DEFAULT 'N/A',
                                                                        `fk_id` VARCHAR (255) DEFAULT 'N/A',
                                                                        `pincode` VARCHAR (255) DEFAULT 'N/A',
                                                                        `url` VARCHAR (255) DEFAULT 'N/A',
                                                                        `name` VARCHAR (255) DEFAULT 'N/A',
                                                                        `availability` VARCHAR (255) DEFAULT 'N/A',
                                                                        `price` VARCHAR (255) DEFAULT 'N/A',
                                                                        `discount` VARCHAR (255) DEFAULT 'N/A',
                                                                        `mrp` VARCHAR (255) DEFAULT 'N/A',
                                                                        PRIMARY KEY (`Id`),
                                                                        UNIQUE KEY `fid` (`fk_id`,`pincode`)
                                                                        ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;
                """
            spider.cursor.execute(create_table)
        except Exception as e:
            print(e)

        try:
            # Create the data table if not exists
            create_table = f"""CREATE TABLE IF NOT EXISTS `page_save_{db.delivery_date}` (`Id` INT NOT NULL AUTO_INCREMENT,
                                                                        `comp` VARCHAR (255) DEFAULT 'N/A',
                                                                        `fk_id` VARCHAR (255) DEFAULT 'N/A',
                                                                        `pincode` VARCHAR (255) DEFAULT 'N/A',
                                                                        `url` VARCHAR (255) DEFAULT 'N/A',
                                                                        `page_save_path` VARCHAR (255) DEFAULT 'N/A',
                                                                        `page_save_id` VARCHAR (255) DEFAULT 'N/A',
                                                                        PRIMARY KEY (`Id`),
                                                                        UNIQUE KEY `fid` (`fk_id`,`pincode`)
                                                                        ) ENGINE = InnoDB DEFAULT CHARSET = UTF8MB4;
                """

            spider.cursor.execute(create_table)
        except Exception as e:
            print(e)

    def process_item(self, item, spider):

        if isinstance(item, QcItem):
            try:

                field_list = []
                value_list = []

                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_db = f"insert into {db.db_data_table}( " + fields + " ) values ( " + values + " )"
                try:
                    spider.cursor.execute(insert_db, tuple(item.values()))
                    spider.con.commit()
                    print('Data Inserted...')
                except IntegrityError as e:
                    print(e)
            except Exception as e:
                print(e)



        elif isinstance(item, QcItem_amz):
            try:
                query = f"""
                            INSERT INTO `page_save_{db.delivery_date}` (
                                comp,
                                fk_id,
                                pincode,
                                url,
                                page_save_path,
                                page_save_id
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                        """
                values = (
                    item["comp"],
                    item["fk_id"],
                    item["pincode"],
                    item["url"],
                    item["page_save_path"],
                    item["page_save_id"]
                )
                spider.cursor.execute(query, values)
                spider.con.commit()
                print('Data Inserted...')

            except Exception as e:
                print(e)
        return item





