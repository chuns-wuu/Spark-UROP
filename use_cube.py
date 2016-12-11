import mechanize
from bs4 import BeautifulSoup
from urlparse import urlparse
from datetime import date
from dateutil.relativedelta import relativedelta
from operator import itemgetter
import cubes, json
from cubes import Workspace, browser

workspace = cubes.Workspace()
workspace.register_default_store("sql",  url="sqlite:///preagg_csv.sqlite")
workspace.import_model("model.json")
cube = workspace.cube("gdelt_ml_actions")

cut = cube.browser.PointCut("date", [1995, 01])
cell = cube.browser.Cell(browser.cube, cuts=[cut])
print cell
