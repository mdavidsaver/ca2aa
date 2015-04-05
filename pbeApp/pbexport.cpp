
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

#include <google/protobuf/stubs/common.h>

struct PBWriter
{
    DataReader& reader;
    // Last returned sample, or NULL if all consumed
    const RawValue::Data *samp;
    const CtrlInfo& info;

    // The year currently being exported
    int year;
    DbrType dtype;
    bool isarray;
    epicsTimeStamp startofyear;
    epicsTimeStamp endofyear;

    std::ofstream outpb;
    int typeChangeError;
    const stdString name;

    PBWriter(DataReader& reader, stdString pv);
    void write(); // all work is done through this method

    void prepFile();

    void skipForward(const char *file);

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
// does this work for array of string? Verified by jbobnar: YES, it works for array of strings
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

    epicsUInt32 disconnected_epoch = 0;
    int prev_severity = 0;
    unsigned long nwrote=0;
    int last_day_fields_written = 0;

    //prepare the field values and write them every day
    //numeric values have all except PREC, which is only for DOUBLE and FLOAT
    //enum has only labels, string has nothing
	std::stringstream ss;
	if (dbr == DBR_TIME_SHORT || dbr == DBR_TIME_INT || dbr == DBR_TIME_LONG || dbr == DBR_TIME_FLOAT
			|| dbr == DBR_TIME_DOUBLE) {
		ss << self.info.getDisplayHigh();
		fieldvalues.push_back(std::make_pair("HOPR",ss.str()));
		ss.str(""); ss.clear(); ss << self.info.getDisplayLow();
		fieldvalues.push_back(std::make_pair("LOPR",ss.str()));
		ss.str(""); ss.clear(); ss << self.info.getUnits();
		fieldvalues.push_back(std::make_pair("EGU",ss.str()));
		if (!isarray) {
			ss.str(""); ss.clear(); ss << self.info.getHighAlarm();
			fieldvalues.push_back(std::make_pair("HIHI",ss.str()));
			ss.str(""); ss.clear(); ss << self.info.getHighWarning();
			fieldvalues.push_back(std::make_pair("HIGH",ss.str()));
			ss.str(""); ss.clear(); ss << self.info.getLowWarning();
			fieldvalues.push_back(std::make_pair("LOW",ss.str()));
			ss.str(""); ss.clear(); ss << self.info.getLowAlarm();
			fieldvalues.push_back(std::make_pair("LOLO",ss.str()));
		}
	}
	if (dbr == DBR_TIME_FLOAT || dbr == DBR_TIME_DOUBLE) {
		ss.str(""); ss.clear(); ss << self.info.getPrecision();
		fieldvalues.push_back(std::make_pair("PREC",ss.str()));
	}
	if (dbr == DBR_TIME_ENUM) {
		stdString state;
		if (self.info.getType() == CtrlInfo::Enumerated) {
			size_t i, num = self.info.getNumStates();
			if (num > 0) {
				self.info.getState(0,state);
				ss <<state.c_str();
				for (i = 1; i < num; i++) {
					self.info.getState(i,state);
					ss << ";" << state.c_str();
				}
				fieldvalues.push_back(std::make_pair("states",ss.str()));
			}
		}
	}

	DbrType previousType = self.reader.getType();
    do{
    	if (self.reader.getType() != previousType) {
    		std::cerr<<"ERROR: The type of PV "<<self.name.c_str()<<" changed from " << previousType << " to " << self.reader.getType() << "\n";
    		std::cerr<<"wrote: "<<nwrote<<"\n";
    		self.typeChangeError += 1;
    		return;
    	}
    	previousType = self.reader.getType();
        sample_t *sample = (sample_t*)self.samp;

        if(sample->stamp.secPastEpoch>=self.endofyear.secPastEpoch) {
            std::cerr<<"Year boundary "<<sample->stamp.secPastEpoch<<" "<<self.endofyear.secPastEpoch <<"\n";
            std::cerr<<"wrote: "<<nwrote<<"\n";
            self.typeChangeError = 0;
            return;
        }
        unsigned int secintoyear = sample->stamp.secPastEpoch - self.startofyear.secPastEpoch;

        encoder.Clear();

        int write_fields = 0;
        int day = sample->stamp.secPastEpoch / 86400;
        if (day != last_day_fields_written) {
        	//if we switched to a new day, write the fields
        	write_fields = 1;
        }

        dbr_short_t sevr = sample->severity;

		if ((sevr == 3904) || (sevr == 3872) || (sevr == 3848)) {
			if (disconnected_epoch == 0) {
				disconnected_epoch = sample->stamp.secPastEpoch;
			}
			if ((sevr == 3872 || sevr == 3848) && prev_severity < 4) {
				prev_severity = sevr;
			}
			write_fields = 0; //don't write fields if disconnected
			continue;
		} else if (sevr > 3) {
			//sevr == 3856 || sevr == 3968
			std::cerr<<"WARN: Severity "<< sevr<<" encountered\n";
			write_fields = 0; //don't write fields if special severity
		} else if (disconnected_epoch != 0) {
			//this is the first sample with value after a disconnected one
			EPICS::FieldValue* FV(encoder.add_fieldvalues());
			std::stringstream str; str << (disconnected_epoch + POSIX_TIME_AT_EPICS_EPOCH);
			FV->set_name("cnxlostepsecs");
			FV->set_val(str.str());

			EPICS::FieldValue* FV2(encoder.add_fieldvalues());
			str.str(""); str.clear(); str << (sample->stamp.secPastEpoch + POSIX_TIME_AT_EPICS_EPOCH);
			FV2->set_name("cnxregainedepsecs");
			FV2->set_val(str.str());

			if (prev_severity == 3872) {
				EPICS::FieldValue* FV3(encoder.add_fieldvalues());
				FV3->set_name("startup");
				FV3->set_val("true");
			} else if (prev_severity == 3848) {
				EPICS::FieldValue* FV3(encoder.add_fieldvalues());
				FV3->set_name("resume");
				FV3->set_val("true");
			}
			prev_severity = sevr;
			disconnected_epoch = 0;
		}

		if (sevr!=0)
			encoder.set_severity(sample->severity);
        if(sample->status!=0)
            encoder.set_status(sample->status);

        encoder.set_secondsintoyear(secintoyear);
        encoder.set_nano(sample->stamp.nsec);

        valueop<dbr, isarray>::set(encoder, sample, self.reader.getCount());

        if(fieldvalues.size() && write_fields)
        {
            // encoder accumulated fieldvalues for this sample
            for(fieldvalues_t::const_iterator it=fieldvalues.begin(), end=fieldvalues.end();
                it!=end; ++it)
            {
                EPICS::FieldValue* FV(encoder.add_fieldvalues());
                FV->set_name(it->first);
                FV->set_val(it->second);
            }
            //fieldvalues.clear(); // don't clear the fields, we will use them again later
            last_day_fields_written = day;
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
            std::cerr<<"ERROR encoding sample! : "<<e.what()<<"\n";
            encbuf.reset();
            // skip
        }

    }while(self.outpb.good() && (self.samp=self.reader.next()));


    std::cerr<<"End file "<<self.samp<<" "<<self.outpb.good()<<"\n";
    std::cerr<<"Wrote "<<nwrote<<"\n";
}

void PBWriter::skipForward(const char* file)
{
	//find the last sample that was written into the given file and skip forward the reader to the first
	//sample that has a timestamp later than the last sample in the file
	std::ifstream inpstr(file);
	std::string temp;
	char *bfr;
	const char *str;
	//Doesn't matter which data type we choose, because we are interested only into timestamp,
	//which is stored at the beginning of the event, so it's the same for all types.
	//Sometimes protobuf prints a warning, because a value is missing, but the timestamp is still there
	EPICS::VectorString en;

	if (!std::getline(inpstr, temp).good()) return; //payload info; don't care what it is, just make sure it was read
	bool ok;
	int logged = 0;
	while(std::getline(inpstr, temp).good()) {
		str = temp.c_str();
		int l = unescape_plan(str,temp.length());
		bfr = (char*)malloc(sizeof(char) * l);
		unescape(temp.c_str(), temp.length(),bfr,l);
		ok = en.ParseFromString(bfr);
		if (!ok && logged == 0){
			std::cerr<<"WARN: "<<name.c_str()<<": Can't parse the data. Probably value is missing.\n";
			logged++;
		}
		free(bfr);
	}
	inpstr.close();
	unsigned int sec = en.secondsintoyear() + startofyear.secPastEpoch;
	unsigned int nano = en.nano();

	//now skip forward to the first sample that is later than the last event read from the file
	unsigned int sampseconds = samp->stamp.secPastEpoch;
	while((sampseconds < sec) && samp) {
		samp = reader.next();
		if (samp)
			sampseconds = samp->stamp.secPastEpoch;
	}

	if (samp && (sampseconds == sec)) {
		unsigned int sampnano = samp->stamp.nsec; //in some cases I got overflow!?
		while (samp && (sampseconds == sec && sampnano <= nano)) {
			samp = reader.next();
			if (samp) {
				sampseconds = samp->stamp.secPastEpoch;
				sampnano = samp->stamp.nsec;
			}
		}
	}
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

    std::cerr<<"is a "<<(isarray?"array\n":"scalar\n");
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
    if (typeChangeError > 0) {
    	fname << pvpathname(reader.channel_name.c_str())<<":"<<year<<".pb."<<typeChangeError;
    } else {
    	fname << pvpathname(reader.channel_name.c_str())<<":"<<year<<".pb";
    }

    int fileexists = 0;
    {
        FILE *fp = fopen(fname.str().c_str(), "r");
        if(fp) {
        	fclose(fp);
        	fileexists = 1;
        	skipForward(fname.str().c_str());
            //std::cerr<<"ERROR: File already exists! "<<fname.str()<<"\n";
            //samp=NULL;
            //return;
        }
    }

    std::cerr<<"Starting to write "<<fname.str()<<"\n";
    createDirs(fname.str());

    escapingarraystream encbuf;
    {
    	if (!fileexists) {
			google::protobuf::io::CodedOutputStream encstrm(&encbuf);
			header.SerializeToCodedStream(&encstrm);
    	}
    }
    encbuf.finalize();

    outpb.open(fname.str().c_str(), std::fstream::app);
    if (!fileexists) { //if file exists do not write header
    	outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
    }
}

PBWriter::PBWriter(DataReader& reader, stdString pv)
    :reader(reader)
	,info(reader.getInfo())
	,year(0)
    ,name(pv)
{
    samp = reader.get();
}

void PBWriter::write()
{
	typeChangeError = 0;
    while(samp) {
    	try {
			prepFile();
			if (!samp) break;
			(*transcode)(*this);
    	} catch (std::exception& up) {
    		if (std::strstr(up.what(),"Error in data header")) {
    			//Error in the data header means a corrupted sample data.
    			//It can happen in the prepFile or in the transcode. Either way the resolution is the same.
    			//We try to move ahead. If it doesn't work, abort.
    			std::cerr<<"ERROR: "<<name.c_str()<<": Corrupted header, continuing with the next sample.\n"<<up.what()<<"\n";
    			samp = reader.next();
    		} else {
    			//tough luck
    			outpb.close();
    			throw std::runtime_error(up.what());
    		}
    	}
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
	//comment this if you want to see the protobuf logs
	google::protobuf::LogSilencer *silencer = new google::protobuf::LogSilencer();

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
    	try {
			if(stdpvname=="<>exit")
				break;
			stdString pvname(stdpvname.c_str());

			std::cerr<<"Got "<<stdpvname<<"\n";

			std::cerr<<"Visit PV "<<pvname.c_str()<<"\n";
			stdString dirname;
			AutoPtr<RTree> tree(idx.getTree(pvname, dirname));

			epicsTime start,end;
			if(!tree || !tree->getInterval(start, end)) {
				std::cerr<<"WARN: No Data or no times\n";
                std::cout<<"Done\n";
				continue;
			}

			std::cerr<<" start "<<start<<" end   "<<end<<"\n";

			AutoPtr<DataReader> reader(ReaderFactory::create(idx, ReaderFactory::Raw, 0.0));

			std::cerr<<" Type "<<reader->getType()<<" count "<<reader->getCount()<<"\n";

			if(!reader->find(pvname, &start)) {
				std::cerr<<"WARN: No data after all\n";
                std::cout<<"Done\n";
				continue;
			}

			PBWriter writer(*reader,pvname);
			writer.write();
    	} catch (std::exception& e) {
    		//print exception and continue with the next pv
    		std::cerr<<"Exception: "<<stdpvname.c_str()<<": "<<e.what()<<"\n";
    	}
        std::cerr<<"Done\n";
        std::cout<<"Done\n"; // exportall.py uses this
    }

    std::cerr<<"Done\n";
    delete silencer;
    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
