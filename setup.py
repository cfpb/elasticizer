from setuptools import setup

setup(
    author='CFPB',
    author_email='tech@cfpb.gov',
    classifiers=[],
    description=u'A tool that pulls data from an arbitrary source and indexes'\
    ' that data in Elasticsearch.',
    include_package_data=True,
    name='elasticizer',
    packages=['elasticizer'],
    test_suite='tests',
    version='0.1.0',
    zip_safe=False,
)
