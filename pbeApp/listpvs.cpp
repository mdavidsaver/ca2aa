
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
#include <algorithm>

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
    std::vector<stdString> names;

    Index::NameIterator iter;
    if(!idx.getFirstChannel(iter)) {
        std::cerr<<"Empty index\n";
        return 1;
    }
    do {
    	names.push_back(iter.getName());
        //std::cout<<iter.getName().c_str()<<"\n";
    }while(idx.getNextChannel(iter));

    std::sort(names.begin(), names.end());
    for( std::vector<stdString>::const_iterator i = names.begin(); i != names.end(); ++i) {
        std::cout << (*i).c_str() << "\n";
    }
    return 0;
}catch(std::exception& e){
    std::cerr<<"Exception: "<<e.what()<<"\n";
    return 1;
}
}
