"""Base page object.

Locators are `data-testid` only. CSS classes and text content are styling and
copy decisions - binding tests to them means a designer's change breaks the
suite. A `data-testid` is a contract between developer and QA.
"""

from __future__ import annotations

import allure
from playwright.sync_api import Locator, Page, expect


class BasePage:
    path: str = "/"

    def __init__(self, page: Page, base_url: str) -> None:
        self.page = page
        self.base_url = base_url.rstrip("/")

    def open(self) -> BasePage:
        with allure.step(f"Open {self.path}"):
            self.page.goto(f"{self.base_url}{self.path}", wait_until="domcontentloaded")
        return self

    def testid(self, value: str) -> Locator:
        return self.page.locator(f'[data-testid="{value}"]')

    def expect_visible(self, testid: str, timeout: float = 10_000) -> Locator:
        locator = self.testid(testid)
        expect(locator).to_be_visible(timeout=timeout)
        return locator

    def screenshot(self, name: str) -> None:
        allure.attach(
            self.page.screenshot(full_page=True),
            name=name,
            attachment_type=allure.attachment_type.PNG,
        )
