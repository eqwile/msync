import os
import django

TEST_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)))

DEBUG = False

SECRET_KEY = '2a2q6*sdz08xbw9@2_tz1lpa-c2$q6xg+#)+gc8p8$dfw4s55('

STATIC_URL = '/static/'

STATIC_ROOT = os.path.join(TEST_DIR, 'static')

TEMPLATE_DIRS = (
    os.path.join(TEST_DIR, 'test_templates'),
)

PASSWORD_HASHERS = (
    'django.contrib.auth.hashers.UnsaltedMD5PasswordHasher',
)

MIDDLEWARE_CLASSES = []
