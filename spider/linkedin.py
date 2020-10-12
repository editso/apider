import logging
from os import path
from urllib import parse

from selenium.webdriver import Chrome, TouchActions, ChromeOptions, ActionChains, Remote
from selenium.webdriver.remote.webelement import WebElement

from .spider import Spider, Cache, ElasticStorage, Storage, make_crawl_log
import spider
from .cache import ElasticCache
from .utils import *
from .selector import Selector
import re
import time

from account import AccountManager
# from scheduler import Response, RemoteClientHandler, JsonDeCoder, JsonEnCoder


class LinkedAccount(AccountManager):

    def __init__(self, *args, **kwargs):
        super().__init__()
        self._index_name = "linkedin_account"
        self._storage: ElasticStorage = ElasticStorage(*args, **kwargs)

    def add(self, account, password, ignore_ivalid=False):
        if not account:
            return
        data = {
            "password": password,
            "account": account,
            "stat": True
        }
        account_info = self._storage.get(
            self._index_name, account, _source=True)
        if not account_info:
            self._storage.save(self._index_name, e_id=account, data=data)
        elif account_info['_source']['password'] != password or ignore_ivalid:
            self._storage.update(self._index_name, e_id=account, body=data)

    def _get_account(self):
        return self._storage.term_query(
            self._index_name, {"stat": True}, _source=True, size=1)

    def get(self):
        data = None
        while not data or data['hits']['total']['value'] <= 0:
            time.sleep(2)
            data = self._get_account()
        return data['hits']['hits'][0]['_source']

    def invalid(self, account):
        if not self._storage.get(self._index_name, e_id=account):
            return
        self._storage.update(self._index_name, e_id=account, body={
            "stat": False
        })


class LinkedinUserInfo(object):
    class Element(object):

        def __setattr__(self, key, value):
            self.__dict__[key] = value.text if value and isinstance(
                value, WebElement) else value

        def to_json(self):
            return self.__dict__

    class Jobs(Element):
        """
        工作信息
        """
        # 公司
        name = None

        # 所在地
        address = None

        # 在职时间
        job_date = None

        # 入职时间
        start_date = None

        # 职位头衔
        end_date = None

        # 职位头衔
        job_title = None

        desc = None

        def __repr__(self):
            return self.__str__()

        def __setattr__(self, key, value):
            self.__dict__[key] = value.text if isinstance(
                value, WebElement) else value

        def __str__(self):
            return "{}#{}#{}".format(self.name, self.start_date, self.job_date)

        def to_json(self):
            return {
                'name': self.name,
                'address': self.address,
                'job_date': self.job_date,
                'start_date': self.start_date,
                'job_title': self.job_title,
                'desc': self.desc
            }

    class Education(Element):
        """
        教育经历
        """
        name = None
        date = None
        degree = None
        pro = None

        def __setattr__(self, key, value):
            self.__dict__[key] = value.text if isinstance(
                value, WebElement) else value

        def __repr__(self):
            return self.__str__()

        def __str__(self):
            return "{}#{}".format(self.name, self.date)

        def to_json(self):
            return {
                'name': self.name,
                'date': self.date,
                'degree': self.degree,
                'pro': self.pro
            }

    class Contact(Element):
        name = None
        address = None

        def __repr__(self):
            return self.__str__()

        def __str__(self):
            return "{}@{}".format(self.name, self.address)

        def to_json(self):
            return {
                'name': self.name,
                'address': self.address
            }

    class Recommendation(Element):
        """
        推荐信
        """
        name = None
        desc = None
        text = None

    class Skill(Element):
        title = None
        name = None
        number = None
        url = None

    def __init__(self, driver: Chrome,
                 selector: Selector = None,
                 max_sleep_sec=10):
        self.driver = driver
        self.url = self.driver.current_url
        self.touch = TouchActions(self.driver)
        self.action = ActionChains(self.driver)
        self.url_parse = parse.urlparse(self.url)
        self.selector = selector or Selector(self.driver)
        self.max_sleep_sec = 10  # 最长睡眠时间
        self.touch.scroll(0, 5000).perform()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug("Crawl Finish: %s" % self.url)

    def close_pane(self):
        pane = self.selector.by_xpath(
            '//button[@class="msg-overlay-bubble-header__control msg-overlay-bubble-header__control--new-convo-btn artdeco-button artdeco-button--circle artdeco-button--inverse artdeco-button--1 artdeco-button--tertiary ember-view"][last()]')
        if pane:
            self.click_element(pane)

    def scroll(self, x, y):
        self.touch.scroll(x, y).perform()

    def scroll_to_element(self, element):
        self.driver.execute_script(
            "arguments[0].scrollIntoView(false)", element)

    def scroll_to_element_by_top(self, element):
        self.driver.execute_script("arguments[0].scrollIntoView()", element)

    def click_element(self, element, sleep=None):
        self.driver.execute_script("arguments[0].click()", element)
        self.sleep(sleep)

    def sleep(self, sec=None):
        sleep_range(sec or self.max_sleep_sec)

    def get_name(self):
        return self.selector.by_xpath('//*[@id="ember59"]/div[2]/div[2]/div[1]/ul[1]/li[1]').text

    def get_description(self):
        show_el = self.selector.by_xpath(
            '//*[@id="line-clamp-show-more-button"]')
        self.close_pane()
        if show_el:
            self.click_element(show_el, 5)
        el = self.selector.by_xpath(
            '//p[@class="pv-about__summary-text mt4 t-14 ember-view"]', nullable=True)
        return el.text if el else el

    def _show_all_jobs(self, click_show=False):
        items = self.selector.by_all_xpath(
            '//h2[text()= "工作经历"]/../../ul/../div/button')
        if items.__len__() <= 0:
            return
        for item in items:
            self.scroll_to_element(item)
            if re.match(r'显示*', item.text):
                click_show = True
                self.click_element(item, 5)
            elif re.match(r'收起*', item.text) or not click_show:
                return
        self._show_all_jobs(click_show)

    def get_jobs(self):
        jobs = []
        self._show_all_jobs()
        items = self.selector.by_all_xpath('//h2[text()="工作经历"]/../../ul/li')
        for item in items:
            job = self.Jobs()
            self.scroll_to_element(item)
            job.name = self.selector.by_xpath(
                'section//span[1][text()="公司名称"]/../span[2]|section//p[text()="公司名称"]/../p[2]',
                with_element=item)
            job.start_date = self.selector.by_xpath('section//span[1][text()="入职日期"]/../span[2]',
                                                    with_element=item).text
            job.job_date = self.selector.by_xpath('section//span[1][text()="任职时长"]/../span[2]',
                                                  with_element=item)
            job.job_title = self.selector.by_xpath('section//span[1][text()="职位头衔"]/../span[2]',
                                                   with_element=item)
            job.address = self.selector.by_xpath(
                'section//span[text()="所在地点"]/../span[2]', with_element=item)
            job.desc = self.selector.by_xpath(
                'section//div/div/p', with_element=item)
            job.desc = job.desc.replace("展开", "") if job.desc else job.desc
            jobs.append(job.to_json())
        return jobs

    def get_education(self):
        educations = []
        items = self.selector.by_all_xpath('//h2[text()="教育经历"]/../../ul/li')
        for item in items:
            self.scroll_to_element(item)
            edu = self.Education()
            edu.name = self.selector.by_xpath('div/div//h3', with_element=item)
            edu.degree = self.selector.by_xpath(
                'div/div//p/span[text()="学位"]/../span[2]', with_element=item)
            edu.pro = self.selector.by_xpath(
                'div/div//p/span[text()="专业"]/../span[2]', with_element=item)
            edu.date = self.selector.by_xpath(
                'div/div//p/span[text()="在读时间或预计毕业时间"]/../span[2]', with_element=item)
            educations.append(edu.to_json())
        return educations

    def get_contact(self):
        contacts = []
        contact_el = self.driver.find_element_by_link_text("联系方式")
        self.scroll_to_element(contact_el)
        self.click_element(contact_el, 5)
        contact_el = self.driver.find_element_by_xpath(
            '//div[@class="pv-profile-section__section-info section-info"]')
        items = contact_el.find_elements_by_css_selector(
            'section.pv-contact-info__contact-type')
        for item in items:
            contact = self.Contact()
            contact.name = item.find_element_by_xpath(
                'header[@class="pv-contact-info__header t-16 t-black t-bold"]').text
            contact.address = item.find_element_by_css_selector(
                'header+*').text
            contacts.append(contact.to_json())
        self.selector.by_xpath(
            '//button[@class="artdeco-modal__dismiss artdeco-button artdeco-button--circle artdeco-button--muted artdeco-button--2 artdeco-button--tertiary ember-view"]').click()
        return contacts

    def get_follower(self):
        url = path.join(self.url, "detail/interests/influencers/")
        self.sleep(5)
        self.driver.get(url)
        followers = []
        follower_el = self.selector.by_xpath(
            '//ul[@class="entity-list row"]/../../div/../../div')
        self.selector.scroll_lazy_load(follower_el)
        items = follower_el.find_elements_by_css_selector(
            'li.entity-list-item')
        for item in items:
            followers.append(item.find_element_by_css_selector(
                'a.pv-interest-entity-link').get_attribute('href'))
        self.selector.by_xpath(
            '//button[@class="artdeco-modal__dismiss artdeco-button artdeco-button--circle artdeco-button--muted artdeco-button--2 artdeco-button--tertiary ember-view"]').click()
        return followers

    def get_avatar(self):
        a_el = self.selector.by_xpath(
            '//img[@class="pv-top-card__photo presence-entity__image EntityPhoto-circle-9 lazy-image ember-view"]')
        return image_base64(a_el.get_attribute('src')) if a_el else None

    def _show_all_recommendation(self, click_show=False):
        items = self.selector.by_all_xpath(
            '//h2[text()="推荐信"]/../../div/div//button[text()="展开"]|//h2[text()="推荐信"]/../../div/div//button')
        if items.__len__() < 0:
            return
        btn = None
        for item in items:
            btn = item
            if re.match('展开', btn.text):
                click_show = True
                break
            elif re.match('其他.*', btn.text):
                break
            elif re.match('收起.*', btn.text) or not click_show:
                return
        if btn:
            try:
                self.scroll_to_element(btn)
                self.click_element(btn, 5)
            except Exception:
                pass
        self._show_all_recommendation(click_show)

    def _get_recommendation(self, name="已收到"):
        btn = None
        items = self.selector.by_all_xpath('//h2[text()="推荐信"]/../..//button')
        for item in items:
            if re.match('.*{}.*'.format(name), item.text):
                btn = item
        if not btn:
            return []
        self.scroll_to_element(btn)
        self.click_element(btn, 5)
        self._show_all_recommendation()
        items = self.selector.by_all_xpath(
            '//h2[text()="推荐信"]/../../div/div//ul/li')
        all_rec = []
        self.close_pane()
        for item in items:
            rec = self.Recommendation()
            rec.name = self.selector.by_xpath(
                'div/a/div//h3', with_element=item)
            rec.desc = self.selector.by_xpath('div/a//p', with_element=item)
            show = self.selector.by_xpath(
                'div/blockquote//a[text()="更多"]', with_element=item)
            self.scroll_to_element_by_top(item)
            window = self.selector.get_window_size()
            logging.debug('window: {}'.format(window))
            if show:
                self.click_element(show, 5)
            rec.text = self.selector.by_xpath(
                'div//blockquote', with_element=item)
            if rec.text:
                rec.text = rec.text.replace('收起', '')
            all_rec.append(rec.to_json())
        return list(filter(lambda item: item['name'] or item['desc'] or item['text'], all_rec))

    def get_recommendation(self):
        """
        推荐信
        """
        return {
            'receive': self._get_recommendation('已收到'),
            'send': self._get_recommendation('已发出')
        }

    def get_cert(self, url):
        self.sleep()
        self.driver.get(url)
        lazy_el = self.selector.by_xpath(
            '//div[@class="artdeco-modal__content ember-view"]')
        if not lazy_el:
            return
        self.selector.scroll_lazy_load(lazy_el)
        items = self.selector.by_all_xpath('div//a', with_element=lazy_el)
        data = []
        for item in items:
            data.append(item.get_attribute('href'))
        return data

    def get_skill(self):
        show = self.selector.by_xpath('//h2[text()="技能认可"]/../../..//button')
        if show and re.match('.*展开.*', show.text):
            self.click_element(show, 5)
        items = self.selector.by_all_xpath('//h2[text()="技能认可"]/../../..//ol')
        skill = []
        for item in items:
            item_lis = self.selector.by_all_xpath('li', with_element=item)
            title = self.selector.by_xpath(
                '//ol/../h3|//div/h2[text()="技能认可"]', with_element=item)
            data = {
                'title': title.text if isinstance(title, WebElement) else title,
                'skill': []
            }
            for li in item_lis:
                m_skill = self.Skill()
                m_skill.name = self.selector.by_xpath(
                    'div/div//a/span[1]', with_element=li)
                m_skill.number = self.selector.by_xpath(
                    'div/div//a/span[2]', with_element=li)
                m_skill.url = self.selector.by_xpath(
                    'div/div//a', with_element=li).get_attribute('href')
                data['skill'].append(m_skill.to_json())
            skill.append(data)
        return skill

    def to_json(self):
        return {
            'avatar': self.get_avatar(),
            'target': self.url,
            'address': self.get_contact(),
            'desc': self.get_description(),
            'jobs': self.get_jobs(),
            'education': self.get_education()
        }


class LinkedinAdapter(object):

    def get_account(self) -> AccountManager:
        pass

    def get_cache(self) -> Cache:
        pass

    def get_storage(self) -> Storage:
        pass

    def get_driver(self):
        pass


class Linkedin(Spider):

    def __init__(self,
                 page,
                 adapter: LinkedinAdapter,
                 debug=False,
                 cookie_path="./",
                 cookie_file_name="linkedin.json"):
        super().__init__("linkedin",  page)
        self._adapter = adapter
        self.debug = debug
        self.cookie_path = cookie_path
        self.cookie_file_name = cookie_file_name
        self.driver = self._adapter.get_driver()
        self.selector = Selector(self.driver)
        self.page = page

    def quit(self):
        if self.driver:
            self.driver.quit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        save(self.cookie_path, self.cookie_file_name, json.dumps(
            self.driver.get_cookies(), ensure_ascii=False))
        if not self.debug and self.driver:
            self.quit()

    def load_cookies(self):
        driver = self.driver
        cookies = load_json(self.cookie_path, self.cookie_file_name)
        if isinstance(cookies, (list, set)):
            for cookie in cookies:
                driver.add_cookie(cookie)
        else:
            driver.add_cookie(cookies)

    def check_login(self, call_count=0):
        driver = self.driver
        url = parse.urlparse(driver.current_url)
        account = self._adapter.get_account().get()
        if url.path == '/authwall':
            login = driver.find_element_by_xpath(
                "/html/body/main/div/div/form[2]/section/p/a")
            login.click()
            driver.find_element_by_xpath(
                '//*[@id="login-email"]').send_keys(account['account'])
            driver.find_element_by_xpath(
                '//*[@id="login-password"]').send_keys(account['password'])
            driver.find_element_by_xpath('//*[@id="login-submit"]').click()
        if url.path.startswith("/checkpoint/challengesV2") or driver.current_url != self.page:
            self._adapter.get_account().invalid(account['account'])
            driver.get("https://www.linkedin.com/authwall")
            if call_count == 2:
                return False
            self.check_login(call_count=call_count + 1)
        return True

    def crawl_user_info(self):
        log = make_crawl_log(self.page)
        stat = dynamic_attr(spider.code)
        try:
            if not self.check_login():
                log.code = stat.failure
                log.messgae = "登陆失败"
                return log
        except Exception as e:
            log.code = stat.failure
            log.messgae = "登陆失败"
            return log
        driver = self.driver
        log.method = []
        with LinkedinUserInfo(driver, self.selector) as user:
            storage = self._adapter.get_storage()
            cache = self._adapter.get_cache()
            if not storage or not cache:
                log.code = stat.failure
                log.messgae = '存储服务错误'
                return log
            user_info = None
            recommend = None
            skill = None
            follower = None
            try:
                if not storage.exists(self.name, self.unique_id('user_info')):
                    user_info = user.to_json()
            except Exception as e:
                logging.error("error", exc_info=e)
                log.method.append('user_info')
            try:
                if not storage.exists("{}_recommend".format(self.name), self.unique_id('user_recommend')):
                    recommend = {
                        "person": user.url,
                        "data": user.get_recommendation()
                    }
            except Exception:
                log.method.append('recommend')
            try:
                if not storage.exists("{}_skill".format(self.name), self.unique_id('user_skill')):
                    skill = {
                        "person": user.url,
                        "data": user.get_skill()
                    }
            except Exception:
                log.method.append("skill")
            try:
                follower = user.get_follower()
            except Exception:
                log.method.append("follower")
            try:
                if user_info:
                    storage.save(self.name, user_info,
                                 e_id=self.unique_id('user_info'))
                if recommend:
                    storage.save("{}_recommend".format(self.name),
                                 recommend, e_id=self.unique_id('user_recommend'))
                if skill:
                    storage.save("{}_skill".format(self.name), {
                        "person": user.url,
                        "data": skill
                    }, e_id=self.unique_id("user_skill"))
                if follower:
                    for item in follower:
                        cache.push({
                            'url': item
                        })
            except Exception as e:
                logging.error('Save Error', exc_info=e)
                log.method.append('save')
            if len(log.method) > 0:
                log.code = stat.part
        return log

    def start(self):
        driver = self.driver
        driver.get(self.page)
        self.load_cookies()
        driver.refresh()
        time.sleep(2)
        return self.crawl_user_info()


def linkedin_cache(*args, **kwargs):
    return ElasticCache('linkedin_cache', elastic=kwargs, *args)
