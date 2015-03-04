
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>
#include <string>
#include <stdexcept>

#include <epicsTime.h>
#include <osiFileName.h>

static const char pvseps_def[] = ":-{}";
const char *pvseps = pvseps_def;

static const char pathsep[] = OSI_PATH_SEPARATOR;

// write the PV name part of the path
std::string pvpathname(const char* pvname)
{
    std::string fname(pvname);
    size_t p = 0, s = fname.size();

    while((p=fname.find_first_of(pvseps, p))!=std::string::npos)
    {
        fname[p] = pathsep[0];
        p++;
        if(p==s)
            break;
    }
    return fname;
}

// Recurisvely create (if needed) the directory components of the path
void createDirs(const std::string& path)
{
    size_t p=0;
    while((p=path.find_first_of(pathsep[0], p))!=std::string::npos)
    {
        std::string part(path.substr(0, p));
        p++;
        if(mkdir(part.c_str(), 0755)!=0)
            switch(errno) {
            case EEXIST:
                break;
            default:
                perror("mkdir");
            }
        else
            std::cerr<<"Create directory "<<part<<"\n";
    }
}

// Get the year in which the given timestamp falls
void getYear(const epicsTimeStamp& t, int *year)
{
    time_t sec = t.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    tm result;
    if(!gmtime_r(&sec, &result))
        throw std::runtime_error("gmtime_r failed");
    *year = 1900 + result.tm_year;
}

// Fetch the first second of the given year
void getStartOfYear(int year, epicsTimeStamp* t)
{
    tm op;
    memset(&op, 0, sizeof(op));
    op.tm_mday = 1;
    op.tm_mon = 0;
    op.tm_year = year - 1900;
    op.tm_yday = 1;
    time_t firstsec = timegm(&op);
    t->secPastEpoch = firstsec - POSIX_TIME_AT_EPICS_EPOCH;
    t->nsec = 0;
}

std::ostream& operator<<(std::ostream& strm, const epicsTime& t)
{
    time_t_wrapper sec(t);
    strm<<ctime(&sec.ts);
    return strm;
}
