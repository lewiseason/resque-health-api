import os
import sys

os.chdir(os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(__file__))

import bottle
import app

application = bottle.default_app()
