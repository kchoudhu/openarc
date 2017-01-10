from setuptools import setup

setup(name='openarc',
      version='0.1.0',
      description='Functional reactive graph backed by PostgreSQL',
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
      ],
      keywords='graph orm frp finance trading',
      url='http://github.com/kchoudhu/openarc',
      author='Kamil Choudhury',
      author_email='kamil.choudhury@anserinae.net',
      license='BSD',
      packages=['openarc'],
      install_requires=[
          'gevent',
          'msgpack-python',
          'psycopg2',
          'zmq'
      ],
      include_package_data=True,
      zip_safe=False)
