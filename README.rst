`schedule with persistence <https://schedule.readthedocs.io/>`__
===============================================

## WORK IN PROGRESS! ##

The original version is cassandra db centric and uses an XLSX file copied to each node, for jobs to run. Changes are being made to make the schedule file xlsx, csv or db table.

.. image:: https://github.com/dbader/schedule/workflows/Tests/badge.svg
        :target: https://github.com/dbader/schedule/actions?query=workflow%3ATests+branch%3Amaster

.. image:: https://coveralls.io/repos/dbader/schedule/badge.svg?branch=master
        :target: https://coveralls.io/r/dbader/schedule

.. image:: https://img.shields.io/pypi/v/schedule.svg
        :target: https://pypi.python.org/pypi/schedule

DBader: 'Python job scheduling for humans'. Run Python functions (or any other callable) periodically using a friendly syntax.

This is a windows centric version of dbaders schedule library, with added persistence and recording of when-last-run. The scheduler can also be distributed across several machines, which read their jobs from a central schedule file or database table.

- A simple to use API for scheduling jobs, made for humans.
- In-process scheduler for periodic jobs. No extra processes needed!
- Very lightweight and no external dependencies.
- Excellent test coverage.
- Tested on Python and 3.6, 3.7, 3.8, 3.9
- (+) integration of safe_schedule class
- (+) additions to handle reading a schedule from an xlsx file
- (+) single schedule for multiple nodes
- (+) loading of history from a csv file including time of last run

Usage
-----
You can either pip install from this github repository, or from the repo location that you forked this repo to:
.. code-block:: bash

    (base)C:\Users\Username> pip install 'persistent_schedule @ git+https://github.com/julianrcook/persistent_schedule'
    (base)C:\Users\Username> python run_safe_scheduler.py
    

.. code-block:: python

    import schedule
    import time

    def job():
        print("I'm working...")
    
    schedule.every(10).seconds.do(job)
    schedule.every(10).minutes.do(job)
    schedule.every().hour.do(job)
    schedule.every().day.at("10:30").do(job)
    schedule.every(5).to(10).minutes.do(job)
    schedule.every().monday.do(job)
    schedule.every().wednesday.at("13:15").do(job)
    schedule.every().day.at("12:42", "Europe/Amsterdam").do(job)
    schedule.every().minute.at(":17").do(job)

    def job_with_argument(name):
        print(f"I am {name}")
        
    schedule.every(10).seconds.do(job_with_argument, name="Peter")
        
    while True:
        schedule.run_pending()
        time.sleep(1)

Documentation
-------------

Schedule's documentation lives at `schedule.readthedocs.io <https://schedule.readthedocs.io/>`_.


Meta
----

Daniel Bader - `@dbader_org <https://twitter.com/dbader_org>`_ - mail@dbader.org

Inspired by `Adam Wiggins' <https://github.com/adamwiggins>`_ article `"Rethinking Cron" <https://adam.herokuapp.com/past/2010/4/13/rethinking_cron/>`_ and the `clockwork <https://github.com/Rykian/clockwork>`_ Ruby module.

Distributed under the MIT license. See `LICENSE.txt <https://github.com/dbader/schedule/blob/master/LICENSE.txt>`_ for more information.

https://github.com/dbader/schedule
