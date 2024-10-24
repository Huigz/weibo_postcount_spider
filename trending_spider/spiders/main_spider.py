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
    "SCF": "Alagf-weAQRGtAnfrV829LjMqlXUA9jF34c0UwubC9Ldyf1qTfegm_sQt8reyroQSbv1PZQY5JXl7PqRmO56sl0.",
    "SINAGLOBAL": "25422661811.929092.1720086177256",
    "UOR": ",,weibo.cn",
    "XSRF-TOKEN": "dS1VyKlhQPmzaLgU-xaUf3BB",
    "_s_tentry": "passport.weibo.com",
    "Apache": "5302458890479.685.1728543351823",
    "ULV": "1728543351825:3:1:1:5302458890479.685.1728543351823:1721032381341",
    "ALF": "1732362918",
    "SUB": "_2A25KHkX2DeThGeBK7FIV9CvJzz6IHXVpUsc-rDV8PUJbkNANLRClkW1NR4I0vXxZek_jfTrva1Dtuz83pVka_iS5",
    "SUBP": "0033WrSXqPxfM725Ws9jqgMF55529P9D9WWNjUojElMwvY5e4olrIAoi5JpX5KMhUgL.FoqXS05XSh-fShz2dJLoI7LzUgLrwoe71hMt",
    "WBPSESS": "j1VDLQPMFRM5F_ozctP1q3n8FxLyiiAf5mx_jRuFq-TCw4r2U6_24elfWoxlQgdTkmyOEPJJmsToncWgTQabHGToBUZQPWEDRDZVI8aVUXKOhHyvseve0z4VSJSgMsWIayL2P4i3qFWDWwN5iyElkA==",
    "_tea_utm_cache_10000007": "undefined",
    "PC_TOKEN": "fef792d192",
    "wb_view_log_6470445542": "1470*9562",
    "webim_unReadCount": "%7B%22time%22%3A1729771247209%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22chat_group_notice%22%3A0%2C%22allcountNum%22%3A21%2C%22msgbox%22%3A0%7D"
    }
    
    custom_settings = {
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
        if not response.url.startswith(self.base_url):
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
            self.logger.info(f"page:{self.page}-value{post_time.format("YYYY-MM-DD HH:mm:ss")}")
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

