
#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>


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
#include "pbeutil.h"
#include "EPICSEvent.pb.h"

struct PBWriter
{
    DataReader& reader;
    // Last returned sample, or NULL if all consumed
    const RawValue::Data *samp;
    // The year currently being exported
    int year;
    DbrType dtype;
    bool isarray;
    epicsTimeStamp startofyear;
    epicsTimeStamp endofyear;

    std::ofstream outpb;

    PBWriter(DataReader& reader);
    void write(); // all work is done through this method

    void prepFile();

    void (*transcode)(PBWriter&); // Points to a transcode_samples<>() specialization
};

/* Type information lookup, indexed by DBR_* type, and whether the PV is an array.
 * Looks up:
 *  typename dbrstruct<DBR,isarray>::dbrtype (ie. struct dbr_time_double)
 *  typename dbrstruct<DBR,isarray>::pbtype (ie. class EPICS::ScalarDouble)
 *  dbrstruct<DBR,isarray>::pbcode (an enum PayloadType value cast to int, ie. EPICS::SCALAR_DOUBLE)
 */
template<int dbr, int isarray> struct dbrstruct{};
#define ENTRY(ARR, DBR, dbr, PBC, PT) \
template<> struct dbrstruct<DBR, ARR> {typedef dbr dbrtype; typedef EPICS::PBC pbtype; enum {pbcode=EPICS::PT};}
ENTRY(0, DBR_TIME_STRING, dbr_time_string, ScalarString, SCALAR_STRING);
ENTRY(0, DBR_TIME_CHAR, dbr_time_char, ScalarByte, SCALAR_BYTE);
ENTRY(0, DBR_TIME_SHORT, dbr_time_short, ScalarShort, SCALAR_SHORT);
ENTRY(0, DBR_TIME_ENUM, dbr_time_enum, ScalarEnum, SCALAR_ENUM);
ENTRY(0, DBR_TIME_LONG, dbr_time_long, ScalarInt, SCALAR_INT);
ENTRY(0, DBR_TIME_FLOAT, dbr_time_float, ScalarFloat, SCALAR_FLOAT);
ENTRY(0, DBR_TIME_DOUBLE, dbr_time_double, ScalarDouble, SCALAR_DOUBLE);
ENTRY(1, DBR_TIME_STRING, dbr_time_string, VectorString, WAVEFORM_STRING);
ENTRY(1, DBR_TIME_CHAR, dbr_time_char, VectorChar, WAVEFORM_BYTE);
ENTRY(1, DBR_TIME_SHORT, dbr_time_short, VectorShort, WAVEFORM_SHORT);
ENTRY(1, DBR_TIME_ENUM, dbr_time_enum, VectorEnum, WAVEFORM_ENUM);
ENTRY(1, DBR_TIME_LONG, dbr_time_long, VectorInt, WAVEFORM_INT);
ENTRY(1, DBR_TIME_FLOAT, dbr_time_float, VectorFloat, WAVEFORM_FLOAT);
ENTRY(1, DBR_TIME_DOUBLE, dbr_time_double, VectorDouble, WAVEFORM_DOUBLE);
#undef ENTRY

/* Type specific operations helper for transcode_samples<>().
 *  valueop<DBR,isarray>::set(PBClass, dbr_* pointer, # of elements)
 *   Assign a scalar or array to the .val of a PB class instance (ie. EPICS::ScalarDouble)
 */
template<int dbr, int isarray> struct valueop {
    static void set(typename dbrstruct<dbr,isarray>::pbtype& pbc,
                    const typename dbrstruct<dbr,isarray>::dbrtype* pdbr,
                    DbrCount)
    {
        pbc.set_val(pdbr->value);
    }
};

// Partial specialization for arrays (works for numerics and scalar string)
// TODO: does this work for array of string?
template<int dbr> struct valueop<dbr,1> {
    static void set(typename dbrstruct<dbr,1>::pbtype& pbc,
                    const typename dbrstruct<dbr,1>::dbrtype* pdbr,
                    DbrCount count)
    {
        pbc.mutable_val()->Reserve(count);
        for(DbrCount i=0; i<count; i++)
            pbc.add_val((&pdbr->value)[i]);
    }
};

// specialization for scalar char
template<> struct valueop<DBR_TIME_CHAR,0> {
    static void set(EPICS::ScalarByte& pbc,
                    const dbr_time_char* pdbr,
                    DbrCount)
    {
        char buf[2];
        buf[0] = pdbr->value;
        buf[1] = '\0';
        pbc.set_val(buf);
    }
};

// specialization for vector char
template<> struct valueop<DBR_TIME_CHAR,1> {
    static void set(EPICS::VectorChar& pbc,
                    const dbr_time_char* pdbr,
                    DbrCount count)
    {
        const epicsUInt8 *pbuf = &pdbr->value;
        pbc.set_val((const char*)pbuf);
    }
};

template<int dbr, int isarray>
void transcode_samples(PBWriter& self)
{
    typedef const typename dbrstruct<dbr,isarray>::dbrtype sample_t;
    typedef typename dbrstruct<dbr,isarray>::pbtype encoder_t;
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
    header.set_type((EPICS::PayloadType)dbrstruct<DBR, 0>::pbcode); break
        CASE(DBR_TIME_STRING);
        CASE(DBR_TIME_CHAR);
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
    header.set_type((EPICS::PayloadType)dbrstruct<DBR, 1>::pbcode); break
        CASE(DBR_TIME_STRING);
        CASE(DBR_TIME_CHAR);
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
    {
        char *seps = getenv("NAMESEPS");
        if(seps)
            pvseps = seps;
    }
    AutoIndex idx;
    idx.open(argv[1]);

    std::string stdpvname;
    while(std::getline(std::cin, stdpvname).good()) {
        if(stdpvname=="<>exit")
            break;
        stdString pvname(stdpvname.c_str());

        std::cerr<<"Got "<<stdpvname<<"\n";

        std::cerr<<"Visit PV "<<pvname.c_str()<<"\n";
        stdString dirname;
        AutoPtr<RTree> tree(idx.getTree(pvname, dirname));

        epicsTime start,end;
        if(!tree || !tree->getInterval(start, end)) {
            std::cerr<<"No Data or no times\n";
            continue;
        }

        std::cerr<<" start "<<start<<" end   "<<end<<"\n";

        AutoPtr<DataReader> reader(ReaderFactory::create(idx, ReaderFactory::Raw, 0.0));

        std::cerr<<" Type "<<reader->getType()<<" count "<<reader->getCount()<<"\n";

        if(!reader->find(pvname, &start)) {
            std::cerr<<"No data after all\n";
            continue;
        }

        PBWriter writer(*reader);
        writer.write();
        std::cerr<<"Done\n";
        std::cout<<"Done\n";
    }

    std::cerr<<"Done\n";
    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
