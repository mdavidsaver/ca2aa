
#include <sstream>
#include <algorithm>

#include <iostream>
#include <cstring>

#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/coded_stream.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "pbsearch.h"
#include "pbstreams.h"
#include "pbeutil.h"
#include "EPICSEvent.pb.h"

static void testTime()
{
    testDiag("Test time calculations");
    int year;

    /* 2015-03-04 18:46:20 UTC */
    epicsTimeStamp ts = {1425494780-POSIX_TIME_AT_EPICS_EPOCH, 0};

    getYear(ts, &year);
    testOk1(year==2015);

    epicsTimeStamp ts2 = {1,2};
    getStartOfYear(2015, &ts2);
    testOk(ts2.secPastEpoch+POSIX_TIME_AT_EPICS_EPOCH==1420070400, "%lu",
           (unsigned long)ts2.secPastEpoch+POSIX_TIME_AT_EPICS_EPOCH);
}

static void testEscape()
{
    static const char input[] = "hello\nworld";
    static const char expect[] = "hello\x1b\x02world\n";
    escapingarraystream encbuf;

    testDiag("Testing PB escaping");

    encbuf.pos = sizeof(input)-1;
    encbuf.inbuf.resize(encbuf.pos);
    std::copy(input, input+encbuf.pos, encbuf.inbuf.begin());

    encbuf.finalize();
    testOk1(encbuf.pos==0);
    testOk1(encbuf.inbuf.size()==0);

    testOk1(encbuf.outbuf.size()==sizeof(expect)-1);
    testOk1(std::string(expect)==std::string(&encbuf.outbuf[0], encbuf.outbuf.size()));
}

static void writeSample()
{
    EPICS::ScalarInt encoder;

    encoder.set_secondsintoyear(1234);
    encoder.set_nano(5678);
    encoder.set_val(42);

    testDiag("SerializeAsString");
    std::string outstr(encoder.SerializeAsString());
    testDiag("encoded length %u", (unsigned)outstr.size());

    {
        testDiag("SerializeToOstream");
        std::ostringstream encstrm;
        testOk1(encoder.SerializeToOstream(&encstrm));
        testOk(encstrm.str().size()==outstr.size(), "ostream length %u", (unsigned)encstrm.str().size());
        testOk1(encstrm.str()==outstr);
    }

    {
        testDiag("ArrayOutputStream");
        std::vector<char> buf(22);
        google::protobuf::io::ArrayOutputStream encbuf(&buf[0], buf.size());
        {
            google::protobuf::io::CodedOutputStream encstrm(&encbuf);

            testOk1(encoder.SerializeToCodedStream(&encstrm));
            testOk((size_t)encstrm.ByteCount()==outstr.size(), "ByteCount %u", (unsigned)encstrm.ByteCount());
        }

        testOk((size_t)encbuf.ByteCount()==outstr.size(), "ByteCount %u", (unsigned)encbuf.ByteCount());
    }

    {
        testDiag("escapingarraystream");
        escapingarraystream encbuf;
        {
            google::protobuf::io::CodedOutputStream encstrm(&encbuf);

            testOk1(encoder.SerializeToCodedStream(&encstrm));
            testOk((size_t)encstrm.ByteCount()==outstr.size(), "ByteCount %u", (unsigned)encstrm.ByteCount());
        }
        testOk1(encbuf.pos==outstr.size());
        encbuf.finalize();
        testOk1(encbuf.pos==0);
        testOk1(encbuf.inbuf.size()==0);

        testOk(encbuf.outbuf.size()==outstr.size()+1, "size w/ EOL %u", (unsigned)encbuf.outbuf.size());
        std::string outstr2(&encbuf.outbuf[0], encbuf.outbuf.size());
        testOk1(outstr+"\n"==outstr2);

        {
            google::protobuf::io::CodedOutputStream encstrm(&encbuf);

            testOk1(encoder.SerializeToCodedStream(&encstrm));
            testOk((size_t)encstrm.ByteCount()==outstr.size(), "ByteCount %u", (unsigned)encstrm.ByteCount());
        }
        testOk1(encbuf.pos==outstr.size());
        encbuf.finalize();
        testOk1(encbuf.pos==0);
        testOk1(encbuf.inbuf.size()==0);

        testOk(encbuf.outbuf.size()==outstr.size()+1, "size w/ EOL %u", (unsigned)encbuf.outbuf.size());
        outstr2 = std::string(&encbuf.outbuf[0], encbuf.outbuf.size());
        testOk1(outstr+"\n"==outstr2);
    }
}

static const char* getLastSampleFile()
{
    char const *folder = getenv("TMPDIR");
    if (folder == 0)
        folder = "/tmp";
    std::stringstream ss;
    ss<<folder<<"/lastSample:2015.pb";
    return ss.str().c_str();
}

//data to find the last sample in them
static void genLastSampleData()
{
    EPICS::ScalarInt encoder;
    EPICS::PayloadInfo info;
    std::ofstream outpb;


    info.set_type(EPICS::SCALAR_INT);
    info.set_pvname("lastSample");
    info.set_elementcount(1);
    info.set_year(2015);

    escapingarraystream encbuf;
    {
        google::protobuf::io::CodedOutputStream encstrm(&encbuf);
        info.SerializeToCodedStream(&encstrm);
    }
    encbuf.finalize();

    outpb.open(getLastSampleFile(), std::fstream::out | std::fstream::binary);
    outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());

    int i;
    for (i = 0; i < 5; i++) {
        encoder.Clear();
        encoder.set_secondsintoyear(1234+i);
        encoder.set_nano(5000 + i);
        encoder.set_val(0);
        {
            google::protobuf::io::CodedOutputStream encstrm(&encbuf);
            encoder.SerializeToCodedStream(&encstrm);
        }
        encbuf.finalize();
        outpb.write(&encbuf.outbuf[0], encbuf.outbuf.size());
    }

    outpb.close();

}

static void testFindLastSample()
{
    genLastSampleData();

    EPICS::ScalarInt sample;
    sample = searcher<DBR_TIME_LONG,0>::getLastSample(getLastSampleFile());
    testOk(sample.secondsintoyear() == 1238, "Sample seconds %d",sample.secondsintoyear());
    testOk(sample.nano() == 5004, "Sample nanos %d",sample.nano());

    EPICS::ScalarDouble sampleDouble;
    try {
        sampleDouble = searcher<DBR_TIME_DOUBLE,0>::getLastSample(getLastSampleFile());
        testOk(0,"Should fail the conversion because of mismatching type");
    } catch (std::invalid_argument& e) {
        testOk1(1);
    }
    remove(getLastSampleFile());
}

MAIN(testPB)
{
    testPlan(29);
    testTime();
    testEscape();
    writeSample();
    testFindLastSample();
    return testDone();
}
