
#include <sstream>
#include <algorithm>

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <epicsUnitTest.h>
#include <testMain.h>

#include "pbstreams.h"
#include "EPICSEvent.pb.h"

static void testEscape()
{
    static const char input[] = "hello\nworld";
    static const char expect[] = "hello\x1b\x01world\n";
    escapingarraystream encbuf;

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

MAIN(testPB)
{
    testPlan(24);
    testEscape();
    writeSample();
    return testDone();
}
