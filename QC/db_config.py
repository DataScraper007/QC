from datetime import datetime, timedelta
import pytz

db_host = 'localhost'
db_user = 'root'
db_password = 'actowiz'
db_port = 3306


today = datetime.now(pytz.timezone('Asia/Calcutta'))
delivery_date = str(datetime.today().strftime("%Y%m%d"))

db_name = 'qcg'
db_links_table = 'input'
db_data_table = None

