"""
Uses a selenium grid instance to retrieve graph images and store them as png

Usage: 

>>> from cstar_perf.frontend.lib import screenshot
>>> screenshot.get_graph_png(url="http://cstar.datastax.com/graph?stats=ffbe9cb6-2b31-11e5-af4a-42010af0688f",
                             image_path="my_screenshot.png")

"""

import subprocess
import json
import logging
logging.basicConfig(level=logging.INFO)

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC


def check_docker_service():
    """Checks if the selenium docker instance is running

    Returns a tuple of (container_exists, container_is_running)"""
    # Check if it's already started:
    p = subprocess.Popen(['docker',
                          'inspect',
                          'cstar_perf_selenium'],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        return (False, False)
    data = json.loads(stdout)
    return (True, data[0]['State']['Running'])


def start_selenium_grid(docker_image='selenium/standalone-chrome'):
    container_exists, container_running = check_docker_service()
    if not container_running:
        if container_exists:
            p = subprocess.call(['docker','rm','-f','cstar_perf_selenium'])
        logging.info("Creating cstar_perf_selenium docker container ...")
        p = subprocess.call(['docker',
                             'run',
                             '-d',
                             '-p',
                             '127.0.0.1:4444:4444',
                             '--name',
                             'cstar_perf_selenium',
                             'selenium/standalone-chrome'])
        assert check_docker_service() == (True, True), "Docker failed to create or start container"
    else:
        logging.info("Found running cstar_perf_selenium docker container ...")
        
def get_graph_png(url, image_path, timeout=60):
    start_selenium_grid()

    driver = webdriver.Remote(
        command_executor='http://127.0.0.1:4444/wd/hub',
        desired_capabilities=DesiredCapabilities.CHROME)

    driver.get(url)
    logging.info("Retrieving page, waiting for page to render: {url}".format(url=url))
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "svg_container"))
        )
        driver.save_screenshot(image_path)
        logging.info("Saved image: {image_path}".format(image_path=image_path))
    finally:
        driver.quit()


