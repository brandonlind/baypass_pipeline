"""
Run baypass commands on compute canada servers by creating an artificial queue.

TODO: efiles must have noheader in the name
TODO: create 00.py file to parallelize gfile creation on lab server.
"""

import sys, os, argparse, random, balance_queue
from os import path as op
from tqdm import tqdm as nb
from baypass_scheduler import mkdir, fs, sbatch
from balance_queue import ColorText


def parse_args():
    """Parse input flags."""
    print(ColorText("\nParsing args ...").bold())
    parser = argparse.ArgumentParser(description=None,
                                     add_help=True,
                                     formatter_class=argparse.RawTextHelpFormatter)
    requiredNAMED = parser.add_argument_group('required arguments')

    requiredNAMED.add_argument("-g", "--gfile_dir",
                               required=True,
                               default=None,
                               dest="gfile_dir",
                               type=str,
                               help='''/path/to/gfiles_dir - this directory contains
the gfiles for baypass that will be parallelized. There should be no other files in 
this directory. Each gfile basename must end with "NUM_noheaderidx.txt", where NUM
is a unique numeric identifier for the file. Any text preceding NUM should end with 
and underscore, eg sometext_NUM_noheaderidx.txt.''')

    requiredNAMED.add_argument("-e", "--efile_dir",
                               required=True,
                               default=None,
                               dest="efile_dir",
                               type=str,
                               help='''/path/to/efile_dir - this directory contains
environmental data for baypass, one environment per file. There should be no other 
files in this directory. Each efile should end with "ENV_noheaderidx.txt", where 
ENV is a unique environmental identifier. Any text preceding ENV should end with an 
underscore, eg sometext_ENV_noheader.txt. ENV should not contain an underscore.''')

    requiredNAMED.add_argument("-o", "--omegafile",
                               required=True,
                               default=None,
                               dest="omegafile",
                               type=str,
                               help='''/path/to/omegafile.txt - this is the path to
the covariance matrix to use for structure correction.''')

    requiredNAMED.add_argument("-p", "--poolsizefile",
                               required=True,
                               default=None,
                               dest="poolsizefile",
                               type=str,
                               help='''/path/to/poolsizefile.txt - this is the path to
the poolsizefile required by baypass.''')

    requiredNAMED.add_argument("-r", "--results_dir",
                               required=True,
                               default=None,
                               dest="results_dir",
                               type=str,
                               help='''/path/to/where/you/want/results - this is the 
path to the directory where results, commands, and sbatch files will be created.''')

    requiredNAMED.add_argument("-b", "--baypass_exe",
                               required=True,
                               default=None,
                               dest="baypass_exe",
                               type=str,
                               help='''/path/to/baypass_exe - this is the path to 
the the baypass executable is located. Should be g_baypass or i_baypass.''')

    requiredNAMED.add_argument("--email",
                               required=True,
                               default=None,
                               dest="email",
                               type=str,
                               help='''Email for notifications if pipeline jobs fail.''')

    requiredNAMED.add_argument("--virtual_env_exe",
                               required=True,
                               default=None,
                               dest="virtual_env",
                               type=str,
                               help='''Path to python virtual environment's activate
executable - eg $HOME/py3/bin/activate.''')

    # optional arguments
    parser.add_argument("--nval",
                        required=False,
                        default=50000,
                        dest="nval",
                        type=int,
                        help="The number of iterations for the MCMC. Default 50000.")

    parser.add_argument("--npilot",
                        required=False,
                        default=40,
                        dest="npilot",
                        type=int,
                        help="The number of pilot runs. Default 40.")

    parser.add_argument("--pilotlength",
                        required=False,
                        default=1000,
                        dest="pilotlength",
                        type=int,
                        help="The number of iterations of each pilot run. Default 1000.")
    

    # get args
    args = parser.parse_args()

    # make sure files exist
    nonexistant = []
    for f in [args.omegafile, args.poolsizefile, args.baypass_exe, args.virtual_env]:
        if not op.exists(f):
            nonexistant.append(f)
    if len(nonexistant) > 0:
        print(ColorText('FAIL: the following files do not exist and are required:').fail())
        for f in nonexistant:
            print(ColorText('FAIL: %s' % f).fail())
        print('exiting baypass pipeline')
        exit()

    # make sure dirs exist
    baddirs = []
    for which,d in zip(['gfile_dir', 'efile_dir'],[args.gfile_dir, args.efile_dir]):
        if not op.exists(d):
            if len(baddirs) == 0:
                print(ColorText('FAIL: the following directories do not exist and are required:').fail())
            print(ColorText('FAIL: %s: %s' % (which, d)).fail())
            baddirs.append(d)
    if len(baddirs) > 0:
        print('exiting baypass pipeline')
        exit()

    # create results dir
    resdir = mkdir(args.results_dir)

    return args


def askforinput(msg='Do you want to proceed?', tab='', newline='\n'):
    """Ask for input; if msg is default and input is no, exit."""
    while True:
        inp = input(ColorText(f"{newline}{tab}INPUT NEEDED: {msg} \n{tab}(yes | no): ").warn().__str__()).lower()
        if inp in ['yes', 'no']:
            if inp == 'no' and msg=='Do you want to proceed?':
                print(ColorText('exiting %s' % sys.argv[0]).fail())
                exit()
            break
        else:
            print(ColorText("Please respond with 'yes' or 'no'").fail())
    return inp


def get_files(directory:str, which_kind:str) -> list:
    """Get a list of files - either gfiles or efiles.
    directory - path to directory
    which_kind - either 'gfiles' or 'efiles'
    """
    # get files
    print(ColorText(f'\nGetting a list of {which_kind} ...').bold())
    files = fs(directory, pattern='noheader', dirs=False)
    print(ColorText(f'\tWARN: Found %s {which_kind}' % len(files)).warn())
    askforinput(tab='\t')
    
    # check naming convention
    badfiles = []
    envs = []
    for f in files:
        try:
            identifier = op.basename(f).split("_")[-2]
        except IndexError as e:
            badfiles.append(f)
            continue
        if which_kind == 'gfiles':
            if float(identifier) != int(identifier):
                badfiles.append(f)
        else:
            if identifier not in envs:
                envs.append(identifier)

    # print out envs, check with user
    if which_kind == 'efiles':
        print(f'\tHere are the {len(envs)} environmental identifiers parsed from efiles.')
        print('\t(These should not be excessively long character strings.)')
        for env in envs:
            print('\t', env)
        askforinput(tab='\t')

    # if gfiles do not have numeric identifiers, exit
    if len(badfiles) > 0:
        print(ColorText('\tFAIL: The following files do not match expected convention.').fail())
        for f in badfiles:
            print(ColorText('\tFAIL: %s' % f).fail())

    return files
    
    

def create_cmds(gfiles:list, efiles:list, args) -> list:
    """Create baypass commands, one per gfile."""
    print(ColorText('\nCreating baypass commands ...').bold())
    cmds = {}
    for gfile in nb(gfiles):
        num = op.basename(gfile).split("_")[-2]
        for efile in efiles:
            env = op.basename(efile).split("_")[-2]
            for chain in ['chain_1', 'chain_2', 'chain_3', 'chain_4', 'chain_5']:
                seed = random.randint(1, 100000)
                bname = op.basename(gfile).split("_noheader")[0]
                outprefix = f"{env}_{bname}_{chain}"
                try:
                    assert outprefix not in list(cmds.keys())
                except AssertionError as e:
                    print(ColorText('\tFAIL: There are duplicate outprefixes. Unexpected error.').fail())
                    print(ColorText('\tFAIL: %s' % outprefix).fail())
                    print('exiting baypass pipeline')
                    exit()
                cmd = f'{args.baypass_exe} -gfile {gfile} \
-efile {efile} \
-omegafile {args.omegafile} \
-poolsizefile {args.poolsizefile} \
-outprefix {outprefix} \
-d0yij 16 \
-seed {seed} \
-pilotlength {args.pilotlength} \
-nval {args.nval} \
-npilot {args.npilot} \
-covmcmc'
                cmds[outprefix] = cmd
    return cmds


def create_sbatch(cmds:list, results_dir:str, email:str, virtual_env:str, pipeline_dir:str) -> (list, str):
    """Create .sh files for each cmd in cmds."""
    print(ColorText('\nCreating shfiles ...').bold())

    shdir = mkdir(op.join(results_dir, 'shfiles'))  # dir for shfiles and slurm.out files
    outdir = mkdir(op.join(results_dir, 'results'))  # dir for baypass.out files
    qdir = mkdir(op.join(results_dir, 'queue_dir'))  # artificial queue
    
    # see if any shfiles already exist
    existing = fs(shdir, endswith='.sh')
    if len(existing) == len(cmds):
        print(ColorText('\tThe pipeline has found the expected number of sh files that have \
already been created.').warn())
        inp = askforinput(msg='Would you like to use these shfiles (yes) or overwrite (no)?', tab='\t')
        if inp == 'yes':
            return existing, qdir
    
    shfiles = []
    for outprefix,cmd in nb(cmds.items()):
        shfile = op.join(shdir, f"{outprefix}.sh")
        with open(shfile, 'w') as o:
            text = f'''#!/bin/bash
#SBATCH --job-name={outprefix}
#SBATCH --time=7-00:00:00
#SBATCH --ntasks=1
#SBATCH --mem=100
#SBATCH --output={outprefix}_%j.out
#SBATCH --mail-user={email}
#SBATCH --mail-type=FAIL

# set up slurm queue format
export SQUEUE_FORMAT="%.8i %.8u %.15a %.68j %.3t %16S %.10L %.5D %.4C %.6b %.7m %N (%r)"

# source python virtual environment
source {virtual_env}

# fill up the slurm queue
cd {pipeline_dir}
python baypass_scheduler.py {qdir}

# balance queue
python balance_queue.py $USER {results_dir}

# run baypass command
cd {outdir}

{cmd}
    '''
            o.write("%s" % text)
        shfiles.append(shfile)

    return shfiles, qdir


def create_queue(shfiles:list, qdir:str) -> list:
    """Create symlinks to shfiles to use as an artificial queue."""
    print(ColorText('\nCreating artificial queue ...').bold())

    qfiles = []
    for src in shfiles:
        dst = op.join(qdir, op.basename(src))
        try:
            os.symlink(src, dst)
        except FileExistsError as e:
            pass
        qfiles.append(dst)

    return qfiles


def main():
    # get dirname for baypass pipeline
    pipeline_dir = op.dirname(op.realpath(sys.argv[0]))

    # get input args
    args = parse_args()

    # determine which slurm accounts to use for balancing
    balance_queue.get_avail_accounts(args.results_dir, save=True)

    # get gfiles
    gfiles = get_files(args.gfile_dir, 'gfiles')

    # get efiles
    efiles = get_files(args.efile_dir, 'efiles')

    # create baypass commands
    cmds = create_cmds(gfiles, efiles, args)

    # create sbatch files, artificial queue directory             
    shfiles, qdir = create_sbatch(cmds,
                                  args.results_dir,
                                  args.email,
                                  args.virtual_env,
                                  pipeline_dir)

    # fill up artificial queue
    qfiles = create_queue(shfiles, qdir)

    # schedule 10 jobs from artificial queue (let runnig jobs scheduler more)
    print(ColorText('\nScheduling 10 jobs (pipeline will have running jobs schedule more) ...').bold())
    pids = sbatch(qfiles)

    # done!
    print(ColorText('\nBaypass pipeline has successfully started!').bold().green())




if __name__ == '__main__':
    main()
