#!/usr/bin/env python

import sys, os, os.path, glob
import threading
from Queue import Queue
import subprocess as SP
import re

from signal import signal, SIGPIPE, SIG_DFL 
signal(SIGPIPE,SIG_DFL) 

mydir = os.path.dirname(os.path.abspath(sys.argv[0]))

def getargs():
    import argparse
    P = argparse.ArgumentParser()
    P.add_argument('indexfile', help='Channel Archiver index to export')
    P.add_argument('outdir', help='Directory where output .pb file tree is written')
    P.add_argument('-j', '--parallel', type=int, default=2,
                   help='Number of exporting worker prcesses.  (default 2)')
    P.add_argument('--seps', default=':-{}', help='PV name seperators (default ":-{}")')
    P.add_argument('--progs', default=mydir, help='Directory under which ./bin/*/listpvs helpers are found')
    P.add_argument('--pv', default='^.*$', help='Regular expression: only PVs that match will be exported')
    P.add_argument('--pvlist', default=None, help='Read PVs from file')

    return P.parse_args()

args = getargs()

listpvs = glob.glob(os.path.join(args.progs, 'bin', '*', 'listpvs'))[0]
pbexport = glob.glob(os.path.join(args.progs, 'bin', '*', 'pbexport'))[0]

idxfile = os.path.abspath(args.indexfile)
exportdir = os.path.abspath(args.outdir)

print 'listpvs',listpvs
print 'pbexport',pbexport
print 'indexfile',idxfile
print 'exportdir',exportdir

exportenv = os.environ.copy()
exportenv['NAMESEPS'] = args.seps
print 'seps',args.seps

# pull in the PV list

pvs = SP.check_output([listpvs, idxfile])

jobs = Queue(10)


regex = re.compile(args.pv)

pvlist = None
if args.pvlist is not None:
  with open(args.pvlist, 'r') as f:
    pvlist = [line.strip() for line in f]


def worker():
  slave = SP.Popen([pbexport, idxfile],
                   stdin=SP.PIPE, stdout=SP.PIPE,
                   cwd=exportdir)
  while True:
    # fetch the next PV to process
    pv = jobs.get()
    if pv is None:
      break
    if regex.match(pv) is None:
      continue
    if pvlist is not None and pv not in pvlist:
      continue
    print 'pv',pv
    slave.stdin.write(pv+'\n')
    # wait for worker to complete
    X = slave.stdout.readline().strip()
    if X!='Done':
      print 'Oops',repr(X)
      break
    print 'Done',pv

  slave.stdin.write('<>exit\n') # trigger graceful exit
  print 'Wait for worker exit'
  code = slave.wait()
  print 'Worker exits',code

nworkers = args.parallel
print 'nworkers',nworkers

Ts = [threading.Thread(target=worker) for i in range(nworkers)]

sys.stdout.flush() # sync output so far, the rest will be mangled

[T.start() for T in Ts]

print 'Workers running'

for pv in pvs.splitlines():
  jobs.put(pv)

print 'All jobs queued'

[jobs.put(None) for T in Ts]
print 'Signaled'
[T.join() for T in Ts]
print 'Done'
