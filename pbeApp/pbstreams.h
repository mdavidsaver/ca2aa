
#include <ostream>
#include <vector>

#include <google/protobuf/io/zero_copy_stream.h>

struct escapingarraystream : public google::protobuf::io::ZeroCopyOutputStream
{
    typedef google::protobuf::int64 int64;
    typedef std::vector<char> buffer_t;
    std::vector<char> inbuf, outbuf;
    size_t pos;

    escapingarraystream();

    virtual bool Next(void **data, int *size);
    virtual void BackUp(int count);
    virtual int64 ByteCount() const;

    void finalize();
    void reset()
    {
        inbuf.clear();
        outbuf.clear();
        pos = 0;
    }
};
