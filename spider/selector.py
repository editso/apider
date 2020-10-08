from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement

from .utils import catcher, dynamic_attr
import time


class Selector(object):

    def __init__(self, driver: WebDriver):
        self.driver = driver
        self._root = 'document.body'

    def get_window_size(self):
        return dynamic_attr(self.driver.get_window_size(self.driver.current_window_handle))

    @catcher()
    def find_element(self, by, value, with_element=None, **kwargs):
        if not with_element:
            with_element = self.driver
        return with_element.find_element(by, value)

    def execute_script(self, script, *args):
        return self.driver.execute_script(script, *args)

    def execute_by_value_script(self, script, *args):
        return self.execute_script("return {}".format(script), *args)

    def scroll_height(self, element=None):
        return self.execute_by_value_script('arguments[0].scrollHeight;', element or self._root)

    def scroll_top(self, element):
        return self.execute_by_value_script('arguments[0].scrollTop;', element)

    def can_show_height(self, element=None):
        return self.execute_by_value_script('arguments[0].clientHeight;', element or self._root)

    def scroll_from(self, height, element=None):
        return self.execute_script('arguments[0].scrollTop = arguments[1];', element or self._root, height)

    def click(self, element):
        if not element or not isinstance(element, WebElement):
            return
        self.execute_script('arguments[0].click()', element)

    def scroll_lazy_load(self, element=None, sleep=2):
        if not element or not isinstance(element, WebElement):
            element = self._root
        height = self.scroll_height(element)
        top = self.scroll_top(element)
        while top != height:
            self.scroll_from(height, element)
            time.sleep(sleep)
            height = self.scroll_height(element)
            top = self.scroll_top(element) + self.can_show_height(element)

    def scroll_to_bottom(self):
        self.execute_script('window.scrollTo(0, document.body.scrollHeight);')

    def scroll_to_top(self):
        self.execute_script('window.scrollTo(0, 0);')

    @catcher()
    def find_elements(self, by, value, with_element=None, **kwargs):
        if not with_element:
            with_element = self.driver
        return with_element.find_elements(by, value)

    def by_class(self, class_name, **kwargs):
        return self.find_element(By.CLASS_NAME, class_name, **kwargs)

    def by_all_class(self, class_name, **kwargs):
        return self.find_elements(By.CLASS_NAME, class_name, **kwargs)

    def by_xpath(self, xpath, **kwargs):
        return self.find_element(By.XPATH, xpath, **kwargs)

    def by_all_xpath(self, xpath, **kwargs):
        return self.find_elements(By.XPATH, xpath, **kwargs)

    def by_selector(self, selector, **kwargs):
        return self.find_element(By.CSS_SELECTOR, selector, **kwargs)

    def by_all_elector(self, selector, **kwargs):
        return self.find_elements(By.CSS_SELECTOR, selector, **kwargs)

    def by_id(self, element_id, with_element=None, **kwargs):
        return self.find_element(By.ID, element_id, **kwargs)
