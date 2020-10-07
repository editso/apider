from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from .utils import catcher


class Selector(object):

    def __init__(self, driver: WebDriver):
        self.driver = driver

    @catcher()
    def find_element(self, by, value, with_element=None, **kwargs):
        if not with_element:
            with_element = self.driver
        return with_element.find_element(by, value)

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

    def by_id(self, element_id, **kwargs):
        return self.find_element(By.ID, element_id, **kwargs)
