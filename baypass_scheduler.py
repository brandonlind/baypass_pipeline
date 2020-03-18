"""Fill up the slurm queue, based on cluster limitations.

Cedar has unlimited jobs; graham and beluga have 1000 job limits.
"""

import sys, subprocess, os, time, shutil
from os import path as op
from balance_queue import getsq, ColorText


def mkdir(d:str) -> str:
    """Create a directory if it doesn't already exist."""
    if not op.exists(d):
        os.makedirs(d)
    return d


def fs(DIR:str, pattern='', endswith='', startswith='', exclude=None, dirs=None, bnames=False) -> list:
    """Get a list of full path names for files and/or directories in a DIR.

    pattern - pattern that file/dir basename must have to keep in return
    endswith - str that file/dir basename must have to keep
    startswith - str that file/dir basename must have to keep
    exclude - str that will eliminate file/dir from keep if in basename
    dirs - bool; True if keep only dirs, False if exclude dirs, None if keep files and dirs
    bnames - bool; True if return is file basenames, False if return is full file path
    """
    if isinstance(exclude, str):
        exclude = [exclude]
    if dirs is False:
        return sorted([op.basename(f) if bnames is True else f
                       for f in fs(DIR,
                                   pattern=pattern,
                                   endswith=endswith,
                                   startswith=startswith,
                                   exclude=exclude)
                       if not op.isdir(f)])
    elif dirs is True:
        return sorted([op.basename(d) if bnames is True else d
                       for d in fs(DIR,
                                   pattern=pattern,
                                   endswith=endswith,
                                   startswith=startswith,
                                   exclude=exclude)
                       if op.isdir(d)])
    elif dirs is None:
        if exclude is not None:
            return sorted([op.join(DIR, f) if bnames is False else f
                           for f in os.listdir(DIR)
                           if pattern in f
                           and f.endswith(endswith)
                           and f.startswith(startswith)
                           and all([excl not in f for excl in exclude])])
        else:
            return sorted([op.join(DIR, f) if bnames is False else f
                           for f in os.listdir(DIR)
                           if pattern in f
                           and f.endswith(endswith)
                           and f.startswith(startswith)])
    pass


def sbatch(files:list, sleep=0, limit=10) -> list:
    """From a list of .sh files, sbatch them and return associated jobid in a list."""
    if isinstance(files, str):
        files = [files]

    pids = []
    failcount=0
    for file in files[:limit]:
        os.chdir(op.dirname(op.realpath(file)))
        try:
            pid = subprocess.check_output([shutil.which('sbatch'), file]).decode('utf-8').replace("\n", "").split()[-1]
        except subprocess.CalledProcessError as e:
            failcount += 1
            if failcount == 10:
                print('\tSlurm is screwing something up ... assessing pids.')
                if len(pids) > 0:
                    return pids
                else:
                    print('\tSlurm did not schedule any jobs, exiting scheduler.')
                    exit()
        print('\tsbatched %s' % file)
        try:
            os.unlink(file)
        except FileNotFoundError as e:
            pass
        pids.append(pid)
        time.sleep(sleep)

    return pids


def start_scheduler(qdir:str) -> str:
    """Create file to prevent other jobs from running scheduler at the same time."""
    scheduler = op.join(qdir, 'scheduler.txt')
    with open(scheduler, 'w') as o:
        # after creating the file, write job id in case i want to cancel process
        try:
            jobid = os.environ['SLURM_JOB_ID']
        except KeyError as e:
            print(ColorText('\tWARN: creating a dummy SLURM_JOB_ID').warn())
            jobid = '1234'
        o.write("scheduler id = %s" % jobid)
    # double check that the scheduler is correct
    with open(scheduler, 'r') as o:
        text = o.read()
    if not text.split()[-1] == jobid:
        print('\tAnother scheduler is in conflict. Allowing other scheduler to proceed.')
        print('\tExiting scheduler.')
        exit()    

    return scheduler


def delsched(scheduler:str) -> None:
    """Remove scheduling reservation file."""
    try:
        os.remove(scheduler)
    except OSError as e:
        pass
    pass


def main(qdir:str):
    print(ColorText('\nStarting baypass scheduler ...').bold())
    # get the threshold for slurm jobs
    qthresh = 999 if os.environ['CC_CLUSTER'] == 'cedar' else 950
    
    # get current slurm queue
    slurmjobs = getsq()
    
    # determine if any jobs can be scheduled
    qlimit = qthresh - len(slurmjobs)
    if qlimit <= 0:
        print('\tQueue is full, exiting baypass_scheduler.py')
        exit()

    # reserve scheduling permissions to the current job
    scheduler = start_scheduler(qdir)

    # get qfiles
    qfiles = fs(qdir, endswith='.sh')
    
    # fill up the slurm queue
    pids = sbatch(qfiles, limit=qthresh)
    
    # remove reservation
    delsched(scheduler)
    
    pass

if __name__ == '__main__':
    thisfile, qdir = sys.argv
    main(qdir)
