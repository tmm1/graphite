import os, sys

'''
If you're using the example-wsgi-vhost.conf file, this file should go in /opt/graphite/webapp/wsgi/
'''

sys.path.append('/opt/graphite/webapp')
os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite.settings'

import django.core.handlers.wsgi

application = django.core.handlers.wsgi.WSGIHandler()
