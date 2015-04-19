#include <time.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <stdexcept>

#include <db_access.h>
#include "EPICSEvent.pb.h"
#include "pbeutil.h"

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

template<int dbr, int array> struct searcher
{
    static typename dbrstruct<dbr, array>::pbtype getLastSample(const char* file)
    {
        typedef typename dbrstruct<dbr, array>::pbtype decoder;
        dbrstruct<dbr, array> type;

        //find the last sample that was written into the given file and skip forward the reader to the first
        //sample that has a timestamp later than the last sample in the file
        std::ifstream inpstr(file);
        std::string temp;
        decoder sample;

        //payload info; don't care what it is, just make sure it was read
        if (!std::getline(inpstr, temp).good()) {
            std::ostringstream msg;
            msg<<"Cannot decode the file header in "<<file;
            throw std::runtime_error(msg.str());
        }

        EPICS::PayloadInfo info;
        int l = unescape_plan(temp.c_str(), temp.length());
        std::vector<char> buf(l);
        unescape(temp.c_str(), temp.length(), &buf[0], buf.size());
        info.ParseFromString(&buf[0]);

        if ((int) (info.type()) != type.pbcode) {
            std::ostringstream msg;
            msg << "ERROR: The existing file " << file
                    << " is of a different type '" << info.type()
                    << "' than the new data '" << type.pbcode << "\n";
            std::cerr<<msg.str().c_str();
            //self.typeChangeError += 1;
            throw std::invalid_argument("Incompatible data type");
        }
        
//reading just the last few lines in the file can be much faster in case of a large file        
/*
        //try to read just the last few lines to handle really long files faster
        int start, bfr_size;
        start = inpstr.tellg();
        inpstr.seekg(0, std::ios_base::end);
        bfr_size = 1024;
        //if the size of the file is larger than our buffer
        if (inpstr.tellg() > bfr_size) {
            int linecount = 0;
            while (linecount < 3) {
                //find at least 3 lines
                inpstr.seekg(-bfr_size, std::ios_base::cur);
                std::vector<char> buf(bfr_size);
                inpstr.read(&buf[0], bfr_size);
                linecount += std::count(buf.begin(), buf.end(), '\n');
                inpstr.seekg(-bfr_size, std::ios_base::cur);
                if (inpstr.tellg() < start) {
                    //we reached the payload info before finding 3 lines
                    inpstr.seekg(start, std::ios_base::beg);
                    break;
                }
            }
            //in case we found enough lines, we might not be located at the correct position
            //to start parsing the data into a sample, so just read the current line.
            if (linecount > 3) {
                if (!std::getline(inpstr, temp).good()) {
                    inpstr.seekg(start, std::ios_base::beg);
                }
            }
        } else {
            //the file size is smaller than buffer. Scan the entire file.
            inpstr.seekg(start, std::ios_base::beg);
        }
*/        

        int logged = 0;
        while (std::getline(inpstr, temp).good()) {
            int l = unescape_plan(temp.c_str(), temp.length());
            std::vector<char> buf(l);
            unescape(temp.c_str(), temp.length(), &buf[0], buf.size());
            bool ok = sample.ParseFromString(&buf[0]);
            if (!ok && logged == 0) {
                std::cerr << "WARN: " << file
                        << ": Can't parse the data. Probably value is missing.\n";
                logged++;
            }
        }
        inpstr.close();
        return sample;
    }
};

