from setuptools import setup, find_packages


setup(
    name='msync',
    version='0.1',
    description='django-orm and mongoengine synchronizer',
    url='https://github.com/eqwile/msync',
    author='readly',
    license='MIT',
    keywords='django orm mongo mongoengine sync',
    packages=['msync'],
    install_requires=['django', 'mongoengine', 'celery'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
        'Framework :: Django',
    ],
)
