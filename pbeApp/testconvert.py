#!/usr/bin/env python

import os, os.path, re
import datetime, calendar
import unittest

import EPICSEvent_pb2 as pb

pbgentestdata = os.path.join(os.getcwd(), 'pbgentestdata')
listpvs = os.path.join(os.getcwd(), 'listpvs')
pbexport = os.path.join(os.getcwd(), 'pbexport')

# Proto buffer instances for decoding individual samples
_fields = {
    0:pb.ScalarString,
    1:pb.ScalarShort,
    2:pb.ScalarFloat,
    3:pb.ScalarEnum,
    4:pb.ScalarByte,
    5:pb.ScalarInt,
    6:pb.ScalarDouble,
    7:pb.VectorString,
    8:pb.VectorShort,
    9:pb.VectorFloat,
    10:pb.VectorEnum,
    #11:pb.VectorByte, # missing?
    12:pb.VectorInt,
    13:pb.VectorDouble,
}

_unesc = re.compile(r'\x1b(.)')
_unmap = {
    '\x01': '\x1b',
    '\x02': '\x0a',
    '\x03': '\x0d',
}
def _unfn(M):
    return _unmap[M.group(1)]
def unescape(inp):
    return _unesc.sub(_unfn, inp.strip())

class TempDir(object):
    def __init__(self, *args, **kws):
        self.args = (args, kws)
        from shutil import rmtree
        self._rmtree = rmtree
        self.name, self.orig = None, None

    def __enter__(self):
        import tempfile
        args, kws = self.args
        self.name = tempfile.mkdtemp(*args, **kws)
        self.orig = os.getcwd()
        os.chdir(self.name)
        return self.name

    def __exit__(self, A, B, C):
        if not self.name:
            return
        os.chdir(self.orig)
        self._rmtree(self.name)

class TestDate(unittest.TestCase):
    """Run test case in temporery directory
    """
    def prepareDir(self):
        import subprocess as SP
        SP.check_call([pbgentestdata, os.getcwd()+'/index'])

    def cleanupDir(self):
        pass

    def convertPV(self, name):
        import subprocess as SP
        worker = SP.Popen([pbexport, os.getcwd()+'/index'], stdin=SP.PIPE)
        worker.stdin.write(name+'\n')
        worker.stdin.write('<>exit\n')
        self.assertEqual(worker.wait(), 0)
        
    def run(self, *args, **kws):
        with TempDir() as dname:
            self.prepareDir()
            ret = unittest.TestCase.run(self, *args, **kws)
            self.cleanupDir()
            return ret

    def assertPBFile(self, fname, head={}, contents=[]):
        with open(fname, 'r') as F:
            lines = map(unescape, F.readlines())
        self.assertTrue(len(lines)>=1)
        H = pb.PayloadInfo()
        H.ParseFromString(lines[0])
        for k,v in head.iteritems():
            if k=='headers':
                continue
            self.assertEqual(getattr(H,k), v, '%s.%s (%s) != %s'%(H,k,getattr(H,k,None),v))

        FVs = [(FV.name, FV.val) for FV in H.headers]
        self.assertEqual(FVs, head.get("headers", []))
        del FVs

        klass = _fields[H.type]
        sec0 = calendar.timegm(datetime.date(H.year,1,1).timetuple())

        for i,(L,(EV,EM)) in enumerate(zip(lines[1:], contents), 2):
            try:
                A = klass()
                A.ParseFromString(L)
                self.assertEqual(A.val, EV)
                self.assertEqual(A.severity, EM.get('sevr',0))
                self.assertEqual(A.status,   EM.get('stat',0))
                self.assertEqual(sec0+A.secondsintoyear,   EM.get('sec',0))
                self.assertEqual(A.nano,     EM.get('ns',0))

                FVs = [(FV.name, FV.val) for FV in A.fieldvalues]
                self.assertEqual(FVs, EM.get("fv", []))
            except:
                print '!!!!!!!!!Error on line',i
                raise

        self.assertEqual(len(lines[1:]), len(contents))

    def test_string(self):
        self.convertPV('a:string:pv')
        self.assertPBFile('a/string/pv:2015.pb',
            head={'year':2015, 'type':0},
            contents=[
                ('hello', {'sec':1425494780}),
                ('world', {'sec':1425494781}),
                ]
        )

    def test_counter(self):
        self.convertPV('pv-counter')
        self.assertPBFile('pv/counter:2015.pb',
            head={'year':2015, 'type':5},
            contents=[
                (0, {'sec':1425494780, 'ns':0, 'fv':[
                    ('HOPR', '10'),('LOPR', '0'),('EGU', 'tick'),('HIHI', '0'),
                    ('HIGH', '0'),('LOW', '0'),('LOLO', '0'),
                    ]}),
                (1, {'sec':1425494781, 'ns':10}),
                (2, {'sec':1425494782, 'ns':20}),
                (3, {'sec':1425494783, 'ns':30}),
                (4, {'sec':1425494784, 'ns':40}),
                (5, {'sec':1425494785, 'ns':50}),
                (6, {'sec':1425494786, 'ns':60}),
                (7, {'sec':1425494787, 'ns':70}),
                (8, {'sec':1425494788, 'ns':80}),
                (9, {'sec':1425494789, 'ns':90}),
                (10,{'sec':1425494790, 'ns':100}),
                ])

    def test_enum(self):
        self.convertPV('enum:pv')
        self.assertPBFile('enum/pv:2015.pb',
            head={'year':2015, 'type':3},
            contents=[
                (2, {'sec':1425494780, 'fv':[('states','A;B;third')]}),
                (0, {'sec':1425494781}),
                (3, {'sec':1425494782}),
                ])

    def test_disconn(self):
        self.convertPV('pv:discon1')
        self.assertPBFile('pv/discon1:2015.pb',
            head={'year':2015, 'type':6},
            contents=[
                (42, {'sec':1425494780, 'fv':[
                    ('HOPR', '10'),('LOPR', '0'),('EGU', 'tick'),('HIHI', '0'),
                    ('HIGH', '0'),('LOW', '0'),('LOLO', '0'),('PREC', '0'),
                    ]}),
                (42, {'sec':1425494790, 'ns':4000, 'fv':[
                    ('cnxlostepsecs', '1425494785'), ('cnxregainedepsecs', '1425494790')]}),
                ])

    def test_restart(self):
        self.convertPV('pv:restart1')
        self.assertPBFile('pv/restart1:2015.pb',
            head={'year':2015, 'type':6},
            contents=[
                (42, {'sec':1425494780, 'fv':[
                    ('HOPR', '10'),('LOPR', '0'),('EGU', 'tick'),('HIHI', '0'),
                    ('HIGH', '0'),('LOW', '0'),('LOLO', '0'),('PREC', '0'),
                    ]}),
                (42, {'sec':1425494790, 'ns':4000, 'fv':[
                    ('cnxlostepsecs', '1425494785'), ('cnxregainedepsecs', '1425494790'), ('startup', 'true')]}),
                ])

    def test_disable(self):
        self.convertPV('pv:disable1')
        self.assertPBFile('pv/disable1:2015.pb',
            head={'year':2015, 'type':6},
            contents=[
                (42, {'sec':1425494780, 'fv':[
                    ('HOPR', '10'),('LOPR', '0'),('EGU', 'tick'),('HIHI', '0'),
                    ('HIGH', '0'),('LOW', '0'),('LOLO', '0'),('PREC', '0'),
                    ]}),
                (42, {'sec':1425494790, 'ns':4000, 'fv':[
                    ('cnxlostepsecs', '1425494785'), ('cnxregainedepsecs', '1425494790'), ('resume', 'true')]}),
                ])

    def test_repeat(self):
        self.convertPV('pv:repeat1')
        self.assertPBFile('pv/repeat1:2015.pb',
            head={'year':2015, 'type':6},
            contents=[
                (42, {'sec':1425494780, 'fv':[
                    ('HOPR', '10'),('LOPR', '0'),('EGU', 'tick'),('HIHI', '0'),
                    ('HIGH', '0'),('LOW', '0'),('LOLO', '0'),('PREC', '0'),
                    ]}),
                (12, {'sec':1425494785, 'ns':5000, 'sevr':3856}),
                (5, {'sec':1425494785, 'ns':6000, 'sevr':3968}),
                (42, {'sec':1425494790, 'ns':4000}),
                ])

if __name__=='__main__':
    unittest.main()
