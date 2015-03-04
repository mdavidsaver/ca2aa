
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

int main(int argc, char *argv[])
{
    if(argc<2)
        return 2;
try{
    AutoIndex idx;
    idx.open(argv[1]);

    Index::NameIterator iter;
    if(!idx.getFirstChannel(iter)) {
        std::cerr<<"Empty index\n";
        return 1;
    }
    do {
        std::cout<<iter.getName().c_str()<<"\n";
    }while(idx.getNextChannel(iter));

    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
