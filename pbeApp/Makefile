TOP=..

include $(TOP)/configure/CONFIG
#----------------------------------------
#  ADD MACRO DEFINITIONS AFTER THIS LINE
#=============================

PROD_HOST += pbexport
pbexport_SRCS += pbexport.cpp
pbexport_SRCS += EPICSEvent.cpp

PROD_LIBS += Storage Tools ca Com

PROD_SYS_LIBS += protobuf

#===========================

include $(TOP)/configure/RULES
#----------------------------------------
#  ADD RULES AFTER THIS LINE

%.pb.cc %.pb.h: ../%.proto
	protoc -I.. --cpp_out=. $<

pbexport$(OBJ): EPICSEvent.pb.h
EPICSEvent$(OBJ): EPICSEvent.pb.cc
