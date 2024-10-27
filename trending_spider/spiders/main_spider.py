import scrapy
from re import findall, compile
import arrow
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from trending_spider.items import TrendingSpiderItem
from scrapy.loader import ItemLoader
import pdb
import time


class Weibo_PostCount_byMonth_Spider(scrapy.Spider):
    name = "postcount_spider"
    
    Cookie2 = {
  "SINAGLOBAL": "8524350012140.869.1721578038069",
  "SCF": "At25fYeZQrrINx-LnVgsFa5Bnajwi-2MzjMebOcUJBe97vCuavBHnzXjSHRanX2TjMPOu2J0cO3s28oAQC9uN4M.",
  "UOR": ",,www.google.com.hk",
  "_tea_utm_cache_10000007": "undefined",
  "_s_tentry": "-",
  "Apache": "9798299112123.732.1729493698706",
  "ULV": "1729493698708:6:4:1:9798299112123.732.1729493698706:1729307681938",
  "XSRF-TOKEN": "m56uhklM9AVCTIcng6bLw0cs",
  "wb_view_log_6470445542": "1920*10801",
  "wb_view_log_5722434776": "1920*10801",
  "SUB": "_2A25KHLE5DeRhGeFH7lUQ9CvMzjmIHXVpUEzxrDV8PUNbmtB-LUHZkW9NevOlf1Rj2cngjwwqxIRkTpQxIcvCIbiz",
  "SUBP": "0033WrSXqPxfM725Ws9jqgMF55529P9D9W50qsOapKg54JXgmx1CLI545JpX5KzhUgL.FoM4SKMpSh-7SK-2dJLoIp7LxKML1KBLBKnLxKqL1hnLBoMN1K-NeKBfeh-f",
  "ALF": "02_1732267625",
  "WBPSESS": "Dt2hbAUaXfkVprjyrAZT_EVh7ns9ZPk-KEh565kKfCrZ-7RYeNjDVveyYruw4OUd6dHUYCFpRQzxqsCvQkjcuE4_Y4mUrujQzjbxkP02l6O3VLywfy46YZMCFfwyeTRUWEE9nrh4i6JAUz4NM-YdM4uvPhnOfmL2AH0nBcjmUEVP1-bD6kYf_7xb4nRqwWqf8sB1iduxP_zqEpDJQedNXw==",
  "wb_view_log_7957145055": "1920*10801",
  "webim_unReadCount": "%7B%22time%22%3A1729675658919%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A0%2C%22msgbox%22%3A0%7D"
}
    
    custom_settings = {
        'REDIRECT_ENABLED':False,
        'FEEDS':{
            'data/time_%(supertopic_id)s.csv': {
            'format': 'csv',  
            'encoding': 'utf8',  
            'fields': None,  
            'overwrite': False  
            }
        }
    }
        
    def __init__(self, name = None, supertopic_id=None, page=1, **kwargs):
        super().__init__(name, **kwargs)
        
        self.supertopic_id = supertopic_id #"10080830bd93bbb40fcd6010e8fb2e1d4372b6"
        self.base_url = f"https://weibo.com/p/{self.supertopic_id}/super_index"
        self.page = int(page)
        self.now = arrow.now()
        self.retry_times = 0


    def start_requests(self):
        self.url = self.base_url+f"?page={self.page}"
        yield scrapy.Request(self.url, callback=self.parse, cookies=self.Cookie2)


    def __str2time(self, time_string):
        
        sec = compile(r'(\d+)秒前')
        mint = compile(r'(\d+)分钟前')
        hour = compile(r'今天(\d+):(\d+)')
        date = compile(r'(\d+)月(\d+)日(\d+):(\d+)')
        year = compile(r'(\d+)-(\d+)-(\d+) (\d+):(\d+)')

        if year.search(time_string) is not None:
            year, month, day, hr, mi = findall(year, time_string)[0]
            return arrow.get(int(year), int(month), int(day), int(hr), int(mi), 0)
        
        time_string = time_string.replace(' ', '')
        if sec.search(time_string) is not None:
            shift_sec = findall(sec, time_string)[0]
            return self.now.shift(seconds=int(shift_sec))
        elif mint.search(time_string) is not None:
            shift_mint = findall(mint, time_string)[0]
            return self.now.shift(minutes=int(shift_mint))
        elif hour.search(time_string) is not None:
            h, m = findall(hour, time_string)[0]
            return arrow.get(2024, self.now.month, self.now.day, int(h), int(m), 0)
        elif date.search(time_string) is not None:
            month, day, hr, mi = findall(date, time_string)[0]
            return arrow.get(2024, int(month), int(day), int(hr), int(mi), 0)
        else:
            raise CloseSpider("[str2time][Error] can't matching time:".format(time_string))

    def parse(self, response, **kwargs):
        if response.status == 302:
            self.logger.warning(f"sleep 30sec and retry page:{self.page}")
            time.sleep(30)
            yield scrapy.Request(self.url, callback=self.parse, cookies=self.Cookie2, dont_filter=True)
            return
        htmls = response.css('script::text').re(r'html.*')
        time_title = None
        for i in range(len(htmls)-1, 0, -1):
            contents_html = htmls[i]
            s = Selector(text=contents_html)
            time_title = s.xpath("//div[contains(@class, 'WB_from')]//a/text()").get()
            if time_title:
                break
        
        if time_title:
            post_time = self.__str2time(time_title)
            self.retry_times = 0 # reset retry times
            
            #save item
            l = ItemLoader(item=TrendingSpiderItem())
            l.add_value('time', post_time.format("YYYY-MM-DD HH:mm:ss"))
            #self.logger.info(f"page:{self.page}-value{post_time}")
            yield l.load_item()

            #next page
            self.page += 1
            self.url = self.base_url+f"?page={self.page}"
            yield scrapy.Request(self.url, callback=self.parse, cookies=self.Cookie2)
        else: #retry
            if self.retry_times > 5: #retry > 6 next page also fail
                raise CloseSpider("retry > 5[Terminate Spider]")
            elif self.retry_times > 3: # retry>3 retry next page
                self.retry_times += 1
                self.page += 1
                self.url = self.base_url+f"?page={self.page}"
                self.logger.warning(f"retry > 3 try next page; retry_times:{self.retry_times}")
                yield scrapy.Request(self.url, callback=self.parse, cookies=self.Cookie2)
                return
            else: #retry current page
                self.retry_times += 1 
                self.logger.warning(f"sleep 30sec and retry page:{self.page}; retry_times:{self.retry_times}")
                time.sleep(30)
                yield scrapy.Request(self.url, callback=self.parse, cookies=self.Cookie2, dont_filter=True)
                return

