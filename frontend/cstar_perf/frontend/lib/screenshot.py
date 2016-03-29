"""Uses a selenium grid instance to retrieve graph images and store them as png.

Depends on the Selenium client driver and docker setup for the user account running this script.

Usage: 

>>> from cstar_perf.frontend.lib import screenshot
>>> screenshot.get_graph_png(url="http://cstar.datastax.com/graph?stats=ffbe9cb6-2b31-11e5-af4a-42010af0688f",
                             image_path="my_screenshot.png")

The first time this is run, it will download the selenium docker
container, start up the grid service on localhost:4444, fetch the
given url, wait for it to render, then take a screenshot and save it
locally.

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

from PIL import Image
from io import BytesIO

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
        return (False, False, 'fail')
    data = json.loads(stdout)
    return (True, data[0]['State']['Running'], data[0]['NetworkSettings']['IPAddress'])


def start_selenium_grid(docker_image='selenium/standalone-chrome'):
    container_exists, container_running, host = check_docker_service()
    if not container_running:
        if container_exists:
            p = subprocess.call(['docker','rm','-f','cstar_perf_selenium'])
        logging.info("Creating cstar_perf_selenium docker container ...")
        p = subprocess.call(['docker',
                             'run',
                             '-d',
                             '-p',
                             '127.0.0.1:4444:4444',
                             '-v',
                             '/dev/shm:/dev/shm',
                             '--name',
                             'cstar_perf_selenium',
                             docker_image])
        container_exists, container_running, host = check_docker_service()
        assert container_exists == True and container_running == True, "Docker failed to create or start container"
    else:
        logging.info("Found running cstar_perf_selenium docker container ...")
    return host
        
def get_graph_png(url, image_path=None, timeout=60, x_crop=None, y_crop=None):
    host = start_selenium_grid()
    print "Fetching screenshot of url " + url

    driver = webdriver.Remote(
        command_executor="http://" + host + ":4444/wd/hub",
        desired_capabilities=DesiredCapabilities.CHROME)

    driver.get(url)
    logging.info("Retrieving page, waiting for page to render: {url}".format(url=url))
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.ID, "svg_container"))
        )

        imageBytes = driver.get_screenshot_as_png()

        if x_crop is not None or y_crop is not None:
            image = Image.open(BytesIO(imageBytes))

            box = image.getbbox()
            left = 0
            upper = 0
            right = box[2]
            lower = box[3]

            if x_crop is not None:
                right = x_crop
            if y_crop is not None:
                lower = y_crop

            image = image.crop( (left, upper, right, lower) )

            newBytes = BytesIO()
            image.save(newBytes, "PNG")
            imageBytes = newBytes.getvalue()

        if image_path:
            logging.info("Saved image: {image_path}".format(image_path=image_path))
            with open(image_path, 'wb') as f:
                f.write(imageBytes)
        else:
            return imageBytes
    finally:
        driver.quit()


