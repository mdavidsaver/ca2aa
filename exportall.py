#!/usr/bin/env python

import sys, os, os.path, glob
import threading
from Queue import Queue
import subprocess as SP

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

def worker():
  slave = SP.Popen([pbexport, idxfile],
                   stdin=SP.PIPE, stdout=SP.PIPE,
                   cwd=exportdir)
  while True:
    # fetch the next PV to process
    pv = jobs.get()
    if pv is None:
      break
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
