
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <stdexcept>

// Base
#include <epicsVersion.h>
#include <epicsTime.h>
#include <osiFileName.h>
// Tools
#include <AutoPtr.h>
#include <BinaryTree.h>
#include <RegularExpression.h>
#include <epicsTimeHelper.h>
#include <ArgParser.h>
// Storage
#include <IndexFile.h>
#include <DataWriter.h>
#include <CtrlInfo.h>

/* 2015-03-04 18:46:20 UTC */
#define BASETIME (1425494780 - POSIX_TIME_AT_EPICS_EPOCH)

// 10 samples at 1 second spacing
static void genCounter(Index& idx)
{
    stdString name("pv-counter");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_LONG, 1, 1.0, 10));

    dbr_time_long val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.value = 0;
    val.stamp.secPastEpoch = BASETIME;
    writer->add((dbr_time_double*)&val);

    for(size_t i=0; i<10; i++) {
        val.value++;
        val.stamp.secPastEpoch++;
        val.stamp.nsec+=10;
        writer->add((dbr_time_double*)&val);
    }

    writer.assign(0);
}

// A string
static void getString(Index& idx)
{
    stdString name("a:string:pv");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_STRING, 1, 1.0, 10));

    dbr_time_string val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.stamp.secPastEpoch = BASETIME;

    strcpy(val.value, "hello");
    writer->add((dbr_time_double*)&val);

    val.stamp.secPastEpoch++;
    strcpy(val.value, "world");
    writer->add((dbr_time_double*)&val);
}

// an enumeration
static void getEnum(Index& idx)
{
    stdString name("enum:pv");
    CtrlInfo info;

    info.allocEnumerated(3, MAX_ENUM_STATES*MAX_ENUM_STRING_SIZE);
    info.setEnumeratedString(0, "A");
    info.setEnumeratedString(1, "B");
    info.setEnumeratedString(2, "third");
    info.calcEnumeratedSize();

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_ENUM, 1, 1.0, 10));

    dbr_time_enum val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.stamp.secPastEpoch = BASETIME;

    val.value = 2;
    writer->add((dbr_time_double*)&val);

    val.stamp.secPastEpoch++;
    val.value = 0;
    writer->add((dbr_time_double*)&val);

    val.stamp.secPastEpoch++;
    val.value = 3; /* not a defined state */
    writer->add((dbr_time_double*)&val);
}

// A disconnection event
static void getDisconn(Index& idx)
{
    stdString name("pv:discon1");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_DOUBLE, 1, 1.0, 10));

    dbr_time_double val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.value = 42;
    val.stamp.secPastEpoch = BASETIME;
    writer->add((dbr_time_double*)&val);

    val.value = 0;
    val.severity = 3904; // Disconnected
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 5000;
    writer->add((dbr_time_double*)&val);

    val.value = 42;
    val.severity = 0;
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 4000;
    writer->add((dbr_time_double*)&val);
}

// ArchiveEngine restart
static void getRestart(Index& idx)
{
    stdString name("pv:restart1");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_DOUBLE, 1, 1.0, 10));

    dbr_time_double val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.value = 42;
    val.stamp.secPastEpoch = BASETIME;
    writer->add((dbr_time_double*)&val);

    val.value = 0;
    val.severity = 3904; // Disconnected
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 5000;
    writer->add((dbr_time_double*)&val);

    val.value = 0;
    val.severity = 3872; // Archive_Off
    val.stamp.nsec = 6000;
    writer->add((dbr_time_double*)&val);

    val.value = 42;
    val.severity = 0;
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 4000;
    writer->add((dbr_time_double*)&val);
}

// ArchiveEngine disable archiving
static void getDisable(Index& idx)
{
    stdString name("pv:disable1");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_DOUBLE, 1, 1.0, 10));

    dbr_time_double val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.value = 42;
    val.stamp.secPastEpoch = BASETIME;
    writer->add((dbr_time_double*)&val);

    val.value = 0;
    val.severity = 3904; // Disconnected
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 5000;
    writer->add((dbr_time_double*)&val);

    val.value = 0;
    val.severity = 3848; // Archive_Disabled
    val.stamp.nsec = 6000;
    writer->add((dbr_time_double*)&val);

    val.value = 42;
    val.severity = 0;
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 4000;
    writer->add((dbr_time_double*)&val);
}

// ArchiveEngine disable archiving
static void getRepeat(Index& idx)
{
    stdString name("pv:repeat1");
    CtrlInfo info;
    info.setNumeric(0, "tick", 0, 10, 0, 0, 0, 0);

    AutoPtr<DataWriter> writer(new DataWriter(idx, name, info,
                                              DBR_TIME_DOUBLE, 1, 1.0, 10));

    dbr_time_double val;
    val.severity = val.status = 0;
    val.stamp.nsec = 0;

    val.value = 42;
    val.stamp.secPastEpoch = BASETIME;
    writer->add((dbr_time_double*)&val);

    val.value = 12;
    val.severity = 3856; // Repeat
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 5000;
    writer->add((dbr_time_double*)&val);

    val.value = 5;
    val.severity = 3968; // Esc_Repeat
    val.stamp.nsec = 6000;
    writer->add((dbr_time_double*)&val);

    val.value = 42;
    val.severity = 0;
    val.stamp.secPastEpoch += 5;
    val.stamp.nsec = 4000;
    writer->add((dbr_time_double*)&val);
}

int main(int argc, char *argv[])
{
    if(argc<2)
        return 2;
    try{
        IndexFile idx;
        idx.open(argv[1], false);
        genCounter(idx);
        getString(idx);
        getEnum(idx);
        getDisconn(idx);
        getRestart(idx);
        getDisable(idx);
        getRepeat(idx);
        return 0;
    }catch(std::exception& e){
        std::cerr<<"Error: "<<e.what()<<"\n";
        return 1;
    }
}
