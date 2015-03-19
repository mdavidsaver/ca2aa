
#include "pbstreams.h"

escapingarraystream::escapingarraystream()
    :inbuf()
    ,outbuf()
    ,pos(0)
{}

bool escapingarraystream::Next(void **data, int *size)
{
    if(pos>=inbuf.size() || inbuf.size()-pos<32)
        inbuf.resize(inbuf.size()+64);

    size_t more = inbuf.size()-pos;
    *data = &inbuf[pos];
    *size = more;
    pos += more;
    return true;
}

void escapingarraystream::BackUp(int count)
{
    pos-=count;
}

escapingarraystream::int64
escapingarraystream::ByteCount() const
{
    return pos;
}

static size_t plan_size(const char* b, size_t l)
{
    size_t outlen=l;
    for(;l;--l, ++b)
        switch(*b)
        {
        case '\n':
        case '\r':
        case '\x1b':
            outlen++;
        }
    return outlen;
}

void escapingarraystream::finalize()
{
    inbuf.resize(pos);
    outbuf.clear();
    outbuf.resize(plan_size(&inbuf[0], inbuf.size()));
    for(size_t i=0, o=0, s=inbuf.size(); i<s; ++i)
    {
        char c=inbuf[i];
        switch(c)
        {
        case '\n':
        case '\r':
        case '\x1b':
            outbuf[o++] = '\x1b';
            break;
        default:
            outbuf[o++] = c;
            continue;
        }
        switch(c)
        {
        case '\x1b': outbuf[o++] = 1; break;
        case '\n': outbuf[o++] = 2; break;
        case '\r': outbuf[o++] = 3; break;
        }
    }
    outbuf.push_back('\n');
    inbuf.clear();
    pos=0;
}
