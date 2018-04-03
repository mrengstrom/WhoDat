# Django settings for pydat project.
#
# Do not edit this file directly for your configuration. Instead please
# copy custom_settings_example.py to custom_settings.py and edit that.

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

DEBUG = False

SITE_ROOT = os.path.dirname(os.path.realpath(__file__))

HANDLER = 'mongo'

MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DATABASE = 'whois'
COLL_WHOIS = 'whois'

ES_URI = 'localhost:9200'
ES_INDEX_PREFIX = 'pydat'

PROXIES = {}

# Elasticsearch limits regular queries to 10000 entries
LIMIT = 10000

PDNS_SOURCES = {}

# Dynamic Passive DNS settings
# Path to pdns source modules
PDNS_MOD_PKG_BASE = "pdns_sources"

# These keys are the ones we allow you to search on. This list must be
# kept up to date as more searches are allowed.
# Array of tuples (mongo_field_name, Friendly Display Name)
# domainName should always be first
SEARCH_KEYS = [ ('domainName', 'Domain'), 
                ('registrant_name', 'Registrant Name'), 
                ('contactEmail', 'Contact Email'), 
                ('registrant_telephone', 'Telephone')
              ]


ADMINS = (
    # ('Your Name', 'your_email@example.com'),
)

MANAGERS = ADMINS

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.', # Add 'postgresql_psycopg2', 'mysql', 'sqlite3' or 'oracle'.
        'NAME': '',                      # Or path to database file if using sqlite3.
        # The following settings are not used with sqlite3:
        'USER': '',
        'PASSWORD': '',
        'HOST': '',                      # Empty for localhost through domain sockets or '127.0.0.1' for localhost through TCP.
        'PORT': '',                      # Set to empty string for default.
    }
}

# Hosts/domain names that are valid for this site; required if DEBUG is False
# See https://docs.djangoproject.com/en/1.5/ref/settings/#allowed-hosts
ALLOWED_HOSTS = []

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name
# although not all choices may be available on all operating systems.
# In a Windows environment this must be set to your system time zone.
TIME_ZONE = 'America/Chicago'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html
LANGUAGE_CODE = 'en-us'

SITE_ID = 1

# If you set this to False, Django will make some optimizations so as not
# to load the internationalization machinery.
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and
# calendars according to the current locale.
USE_L10N = True

# If you set this to False, Django will not use timezone-aware datetimes.
USE_TZ = True

# Absolute filesystem path to the directory that will hold user-uploaded files.
# Example: "/var/www/example.com/media/"
MEDIA_ROOT = ''

# URL that handles the media served from MEDIA_ROOT. Make sure to use a
# trailing slash.
# Examples: "http://example.com/media/", "http://media.example.com/"
MEDIA_URL = ''

# Absolute path to the directory static files should be collected to.
# Don't put anything in this directory yourself; store your static files
# in apps' "static/" subdirectories and in STATICFILES_DIRS.
# Example: "/var/www/example.com/static/"
STATIC_ROOT = os.path.join(SITE_ROOT, '../extras/www/static')

# URL prefix for static files.
# Example: "http://example.com/static/", "http://static.example.com/"
STATIC_URL = '/static/'

# Additional locations of static files
STATICFILES_DIRS = [
    # Put strings here, like "/home/html/static" or "C:/www/django/static".
    # Always use forward slashes, even on Windows.
    # Don't forget to use absolute paths, not relative paths.
]

#recursively search pydat.pdns_modules for any templates that reside in modules
for dirpath, dirs, files in os.walk(os.path.join(SITE_ROOT, PDNS_MOD_PKG_BASE), topdown=True):
    for dir_ in dirs:
        if "static" in dir_:
            STATICFILES_DIRS.append(os.path.join(dirpath, dir_))

# List of finder classes that know how to find static files in
# various locations.
STATICFILES_FINDERS = [
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
]

# Make this unique, and don't share it with anybody.
SECRET_KEY = 'o=skwv+igf2%#6n&p!nd##w(a*wqugkcq4-2=wugz0(715*!l#'

TEST_RUNNER = 'django.test.runner.DiscoverRunner'

MIDDLEWARE = [
    'django.middleware.common.CommonMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    # Uncomment the next line for simple clickjacking protection:
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'pydat.urls'

# Python dotted path to the WSGI application used by Django's runserver.
WSGI_APPLICATION = 'pydat.wsgi.application'

#add pdns template directories to the django directory list
_TEMPLATE_DIRS_ = [os.path.join(SITE_ROOT, "templates")]

#recursively search pydat.pdns_modules for any templates that reside in modules
for dirpath, dirs, files in os.walk(os.path.join(SITE_ROOT, PDNS_MOD_PKG_BASE), topdown=True):
    for dir_ in dirs:
        if "templates" in dir_:
            _TEMPLATE_DIRS_.append(os.path.join(dirpath, dir_))


#insert the core django template directory at the front of
#django template directory list (django searches directories in order)
_TEMPLATE_DIRS_.insert(0, os.path.join(SITE_ROOT, 'templates'))

TEMPLATES = [
        {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": _TEMPLATE_DIRS_,
        "OPTIONS":{
                "context_processors":[
                            'django.contrib.auth.context_processors.auth',
                            'django.template.context_processors.debug',
                            'django.template.context_processors.i18n',
                            'django.template.context_processors.media',
                            'django.template.context_processors.static',
                            'django.template.context_processors.tz',
                            'django.contrib.messages.context_processors.messages',
                            'django.template.context_processors.csrf'
                ],
                'debug': DEBUG,
            },

        },
]

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.sites',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    # Uncomment the next line to enable the admin:
    # 'django.contrib.admin',
    # Uncomment the next line to enable admin documentation:
    # 'django.contrib.admindocs',
    'pydat',
]

# A sample logging configuration. The only tangible logging
# performed by this configuration is to send an email to
# the site admins on every HTTP 500 error when DEBUG=False.
# See http://docs.djangoproject.com/en/dev/topics/logging for
# more details on how to customize your logging configuration.
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        }
    },
    'loggers': {
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
    }
}

# Import custom settings if it exists
csfile = os.path.join(SITE_ROOT, 'custom_settings.py')
if os.path.exists(csfile):
    execfile(csfile)

#Set the mongo read preference if it isn't already
if HANDLER == 'mongo':
    try:
        MONGO_READ_PREFERENCE
    except NameError:
            from pymongo import ReadPreference
            MONGO_READ_PREFERENCE = ReadPreference.PRIMARY
