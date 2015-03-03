
#include <time.h>
#include <string.h>
#include <errno.h>

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
std::string pvpathname(const char* pvname)
{
    std::string fname(pvname);
    size_t p = 0, s = fname.size();

    while((p=fname.find_first_of(pvseps, p))!=std::string::npos)
    {
        fname[p] = '/';
        p++;
        if(p==s)
            break;
    }
    return fname;
}

void createDirs(const std::string& path)
{
    size_t p=0;
    while((p=path.find_first_of('/', p))!=std::string::npos)
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

void getYear(const epicsTimeStamp& t, int *year)
{
    time_t sec = t.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH;
    tm result;
    if(!gmtime_r(&sec, &result))
        throw std::runtime_error("gmtime_r failed");
    *year = 1900 + result.tm_year;
}

void getStartOfYear(int year, epicsTimeStamp* t)
{
    tm op;
    memset(&op, 0, sizeof(op));
    op.tm_mday = op.tm_mon = 1;
    op.tm_year = year - 1900;
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

static
void exportPV(AutoIndex& index,
              const stdString& name,
              const epicsTime& start)
{
    AutoPtr<DataReader> reader(ReaderFactory::create(index, ReaderFactory::Raw, 0.0));

    const RawValue::Data *samp(reader->find(name, &start));
    if(!samp) {
        std::cerr<<"No data after all\n";
        return;
    }
    int year;
    getYear(samp->stamp, &year);
    std::cerr<<" Type "<<reader->getType()<<" count "<<reader->getCount()<<"\n";

    std::ostringstream fname;
    fname << pvpathname(name.c_str())<<":"<<year<<".pb";
    std::cerr<<"Write "<<fname.str()<<"\n";
    createDirs(fname.str());
}

int main(int argc, char *argv[])
{
    if(argc<2)
        return 2;
try{
    AutoIndex idx;
    idx.open(argv[1]);
    std::cerr<<"Opened "<<argv[1]<<"\n";

    Index::NameIterator iter;
    if(!idx.getFirstChannel(iter)) {
        std::cerr<<"Empty index\n";
        return 1;
    }
    do {
        std::cerr<<"Visit PV "<<iter.getName().c_str()<<"\n";
        stdString dirname;
        AutoPtr<RTree> tree(idx.getTree(iter.getName(), dirname));

        epicsTime start,end;
        if(!tree || !tree->getInterval(start, end)) {
            std::cerr<<"No Data or no times\n";
            continue;
        }

        std::cerr<<" start "<<start<<" end   "<<end<<"\n";

        exportPV(idx, iter.getName(), start);

    }while(idx.getNextChannel(iter));

    std::cerr<<"Done\n";
    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
