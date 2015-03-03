
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

#include "pbstreams.h"
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

struct PBWriter
{
    DataReader& reader;
    const RawValue::Data *samp;
    int year;
    DbrType dtype;
    bool isarray;
    epicsTimeStamp startofyear;
    epicsTimeStamp endofyear;

    std::ofstream outpb;

    PBWriter(DataReader& reader);
    void write();

    void prepFile();

    void (*transcode)(PBWriter&);
};

template<int dbr> struct dbrstruct{};
#define ENTRY(DBR, dbr, PBC, PT) \
template<> struct dbrstruct<DBR> {typedef dbr dbrtype; typedef EPICS::PBC pbtype; enum {pbcode=EPICS::PT};}
//ENTRY(DBR_TIME_CHAR, dbr_time_char, ScalarByte, SCALAR_BYTE);
ENTRY(DBR_TIME_SHORT, dbr_time_short, ScalarShort, SCALAR_SHORT);
ENTRY(DBR_TIME_ENUM, dbr_time_enum, ScalarEnum, SCALAR_ENUM);
ENTRY(DBR_TIME_LONG, dbr_time_long, ScalarInt, SCALAR_INT);
ENTRY(DBR_TIME_FLOAT, dbr_time_float, ScalarFloat, SCALAR_FLOAT);
ENTRY(DBR_TIME_DOUBLE, dbr_time_double, ScalarDouble, SCALAR_DOUBLE);
#undef ENTRY

template<int dbr, int isarray> struct valueop {
    static void set(typename dbrstruct<dbr>::pbtype& pbc,
                    const typename dbrstruct<dbr>::dbrtype* pdbr,
                    DbrCount)
    {
        pbc.set_val(pdbr->value);
    }
};

template<int dbr> struct valueop<dbr,1> {
    static void set(typename dbrstruct<dbr>::pbtype& pbc,
                    const typename dbrstruct<dbr>::dbrtype* pdbr,
                    DbrCount count)
    {
        for(DbrCount i=0; i<count; i++)
            pbc.set_val((&pdbr->value)[i]);
    }
};

//TODO special handling for DBR_TIME_CHAR and DBR_TIME_STRING

template<int dbr, int isarray>
void transcode_samples(PBWriter& self)
{
    typedef const typename dbrstruct<dbr>::dbrtype sample_t;
    typedef typename dbrstruct<dbr>::pbtype encoder_t;
    typedef std::vector<std::pair<std::string, std::string> > fieldvalues_t;


    encoder_t encoder;
    escapingarraystream encbuf;
    fieldvalues_t fieldvalues;

    unsigned long nwrote=0;
    do{
        sample_t *sample = (sample_t*)self.samp;

        if(sample->stamp.secPastEpoch>=self.endofyear.secPastEpoch) {
            std::cerr<<"Year boundary "<<sample->stamp.secPastEpoch<<" "<<self.endofyear.secPastEpoch;
            return;
        }
        unsigned int secintoyear = sample->stamp.secPastEpoch - self.startofyear.secPastEpoch;

        encoder.Clear();

        encoder.set_secondsintoyear(secintoyear);
        encoder.set_nano(sample->stamp.nsec);

        if(sample->severity!=0)
            encoder.set_severity(sample->severity);
        if(sample->status!=0)
            encoder.set_status(sample->status);

        valueop<dbr, isarray>::set(encoder, sample, self.reader.getCount());

        if(fieldvalues.size())
        {
            // encoder accumulated fieldvalues for this sample
            for(fieldvalues_t::const_iterator it=fieldvalues.begin(), end=fieldvalues.end();
                it!=end; ++it)
            {
                EPICS::FieldValue* FV(encoder.add_fieldvalues());
                FV->set_name(it->first);
                FV->set_val(it->second);
            }
            fieldvalues.clear();
        }

        try{
            {
                google::protobuf::io::CodedOutputStream encstrm(&encbuf);
                encoder.SerializeToCodedStream(&encstrm);
            }
            encbuf.finalize();
            self.outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
            nwrote++;
        }catch(std::exception& e) {
            std::cerr<<"Error encoding sample! : "<<e.what()<<"\n";
            encbuf.reset();
            // skip
        }

    }while((self.samp=self.reader.next()) && self.outpb.good());
    std::cerr<<"End file "<<self.reader.next()<<" "<<self.outpb.good()<<"\n";
    std::cerr<<"Wrote "<<nwrote<<"\n";
}

void PBWriter::prepFile()
{
    const RawValue::Data *samp(reader.get());
    getYear(samp->stamp, &year);
    getStartOfYear(year, &startofyear);
    getStartOfYear(year+1, &endofyear);

    dtype = reader.getType();
    isarray = reader.getCount()!=1;

    EPICS::PayloadInfo header;

    std::cerr<<"is a "<<(isarray?"scalar\n":"array\n");
    if(!isarray) {
        // Scalars
        switch(dtype)
        {
#define CASE(DBR) case DBR: transcode = &transcode_samples<DBR, 0>; \
    header.set_type((EPICS::PayloadType)dbrstruct<DBR>::pbcode); break
        //CASE(DBR_TIME_CHAR);
        CASE(DBR_TIME_SHORT);
        CASE(DBR_TIME_ENUM);
        CASE(DBR_TIME_LONG);
        CASE(DBR_TIME_FLOAT);
        CASE(DBR_TIME_DOUBLE);
#undef CASE
        default: {
            std::ostringstream msg;
            msg<<"Unsupported type "<<dtype;
            throw std::runtime_error(msg.str());
        }
        }
    } else {
        // Vectors
        switch(dtype)
        {
#define CASE(DBR) case DBR: transcode = &transcode_samples<DBR, 1>; \
    header.set_type((EPICS::PayloadType)dbrstruct<DBR>::pbcode); break
        //CASE(DBR_TIME_CHAR);
        CASE(DBR_TIME_SHORT);
        CASE(DBR_TIME_ENUM);
        CASE(DBR_TIME_LONG);
        CASE(DBR_TIME_FLOAT);
        CASE(DBR_TIME_DOUBLE);
#undef CASE
        default: {
            std::ostringstream msg;
            msg<<"Unsupported type "<<dtype;
            throw std::runtime_error(msg.str());
        }
        }
    }

    header.set_elementcount(reader.getCount());
    header.set_year(year);
    header.set_pvname(reader.channel_name.c_str());

    std::ostringstream fname;
    fname << pvpathname(reader.channel_name.c_str())<<":"<<year<<".pb";

    {
        FILE *fp = fopen(fname.str().c_str(), "r");
        if(fp) {
            fclose(fp);
            std::cerr<<"File already exists! "<<fname.str()<<"\n";
            samp=NULL;
            return;
        }
    }

    std::cerr<<"Starting to write "<<fname.str()<<"\n";
    createDirs(fname.str());

    escapingarraystream encbuf;
    {
        google::protobuf::io::CodedOutputStream encstrm(&encbuf);
        header.SerializeToCodedStream(&encstrm);
    }
    encbuf.finalize();

    outpb.open(fname.str().c_str());
    outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
}

PBWriter::PBWriter(DataReader& reader)
    :reader(reader)
    ,year(0)
{
    samp = reader.get();
}

void PBWriter::write()
{
    while(samp) {
        prepFile();
        (*transcode)(*this);
        bool ok = outpb.good();
        outpb.close();
        if(!ok) {
            std::cerr<<"Error writing file\n";
            break;
        }
    }
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

        AutoPtr<DataReader> reader(ReaderFactory::create(idx, ReaderFactory::Raw, 0.0));

        std::cerr<<" Type "<<reader->getType()<<" count "<<reader->getCount()<<"\n";

        if(!reader->find(iter.getName(), &start)) {
            std::cerr<<"No data after all\n";
            continue;
        }

        PBWriter writer(*reader);
        writer.write();

    }while(idx.getNextChannel(iter));

    std::cerr<<"Done\n";
    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
