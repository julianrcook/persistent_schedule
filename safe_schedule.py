# -*- coding: utf-8 -*-
""" safe_schedule.py
Created on Thu Apr  1 15:48:47 2021
mlewis safe Schedule.py
An implementation of Scheduler that catches jobs that fail.
For use with https://github.com/dbader/schedule
src=https://gist.github.com/mplewis/8483f1c24f2d6259aef6
 
@author: jcook
"""
import logging
from traceback import format_exc
import datetime
from schedule import Scheduler
 
logger = logging.getLogger('schedule')
 
 
class SafeScheduler(Scheduler):
    """
    An implementation of Scheduler that catches jobs that fail, logs their
    exception tracebacks as errors, optionally reschedules the jobs for their
    next run time, and keeps going.
    Use this to run jobs that may or may not crash without worrying about
    whether other jobs will run or if they'll crash the entire script.
    e.g.
    import time
    from safe_schedule import SafeScheduler
    scheduler = SafeScheduler()
    scheduler.every(3).seconds.do(good_task_1)
    scheduler.every(5).seconds.do(bad_task_1)
    while True:
        scheduler.run_pending()
        time.sleep(1)
    """
 
    def __init__(self, reschedule_on_failure=True):
        """
        If reschedule_on_failure is True, jobs will be rescheduled for their
        next run as if they had completed successfully. If False, they'll run
        on the next run_pending() tick.
        """
        self.reschedule_on_failure = reschedule_on_failure
        super().__init__()
 
    def _run_job(self, job):
        try:
            super()._run_job(job)
        except Exception:
            logger.error(format_exc())
            job.last_run = datetime.datetime.now()
            job._schedule_next_run()