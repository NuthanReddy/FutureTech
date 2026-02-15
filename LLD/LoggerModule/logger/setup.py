from setuptools import setup, find_packages

setup(
    name='logger',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyYAML>=6.0.2'
    ],
    author='Bhavana Pajjuri',
    author_email='bhavanapajjuri93@gmail.com',
    description='This is a logger module which support sync and async and multiple sinks.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Bhavana-Reddy/LoggerModule',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
