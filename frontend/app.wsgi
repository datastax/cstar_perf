import os, sys

app_root = os.path.split(os.path.realpath(__file__))[0]

activate_this = os.path.join(app_root, 'env/bin/activate_this.py')
execfile(activate_this, dict(__file__=activate_this))

sys.path.insert(0, app_root)
from app.app import app as application
