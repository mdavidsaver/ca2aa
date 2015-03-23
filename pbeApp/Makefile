TOP=..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

PROD_HOST += listpvs
listpvs_SRCS += listpvs.cpp

PROD_HOST += pbexport
pbexport_SRCS += pbexport.cpp
pbexport_SRCS += pbstreams.cpp
pbexport_SRCS += pbeutil.cpp
pbexport_SRCS += EPICSEvent.cpp

TESTPROD_HOST += testPB
testPB_SRCS += testPB.cpp
testPB_SRCS += pbstreams.cpp
testPB_SRCS += pbeutil.cpp
testPB_SRCS += EPICSEvent.cpp
TESTS += testPB

PROD_HOST += pbgentestdata
pbgentestdata_SRCS += genTestData.cpp

PROD_LIBS += Storage Tools ca Com

PROD_SYS_LIBS += protobuf

TESTSCRIPTS_HOST += $(TESTS:%=%.t)

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

%.pb.cc %.pb.h: ../%.proto
	protoc -I.. --cpp_out=. $<

pbexport$(OBJ): EPICSEvent.pb.h
EPICSEvent$(OBJ): EPICSEvent.pb.cc
