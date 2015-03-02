
#include <time.h>

#include <string>
#include <ostream>

// Base
#include <epicsVersion.h>
#include <epicsTime.h>
// Tools
#include <AutoPtr.h>
#include <BinaryTree.h>
#include <RegularExpression.h>
#include <epicsTimeHelper.h>
#include <ArgParser.h>
// Storage
#include <SpreadsheetReader.h>
#include <AutoIndex.h>

#include "EPICSEvent.pb.h"

static const char pvseps[] = ":-{}";

// write the PV name part of the path
std::ostream& pvpathname(std::ostream& strm,
                          const std::string& pvname)
{
    size_t p = 0, s = pvname.size();

    std::string fname(pvname);
    while((p=fname.find_first_of(pvseps, p))!=std::string::npos)
    {
        p[p] = '/';
        p++;
        if(p==s)
            break;
    }
    strm << fname;
    return strm;
}

bool getYear(const epicsTimeStamp& t, int *year)
{
    unsigned long sec = t.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    tm result;
    if(!gmtime_r(sec, &result))
        return false;
    *year = tm.tm_year;
    return true;
}

int main(int argc, char *argv[])
{
    return 0;
}
