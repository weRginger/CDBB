/*
 * Ziqi Fan: UMN CS PHD
 * This app designed for analysis large amount of results from CDBB experiments
 * The inputFile contains the results wanted to be analyzed
 * e.g. ./analysis file.txt
 * */

#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <algorithm>    // std::sort
#include <vector>       // std::vector

using namespace std;

int main(int argc, char *argv[]) {
    if(argc != 2) {
        cout<<"USAGE: ./analysis <inputFile>"<<endl;
        return 0;
    }
    ifstream myfile(argv[1]);

    string lineInFile;
    size_t found;
    if(myfile.is_open()) {
        while(myfile.good()) {
            getline (myfile,lineInFile);
            found = lineInFile.find("$$");
            if(found != string::npos) {
                cout<<lineInFile<<endl;
            }
        }
    }

    return 0;
}
