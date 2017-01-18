/*
 * Ziqi Fan: UMN CS PHD
 * This app designed for analysis large amount of results from CDBB experiments
 * The inputFile contains the results wanted to be analyzed
 * e.g. ./analysis file.txt
 * sample line in file.txt:
 *     $$ CKPT Run 3: Elapsed time for writer rank 9 is 2.279051, timeStart 1484497521.737813, timeEnd 1484497524.016864
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

    double result[20][1000];

    if(myfile.is_open()) {
        while(myfile.good()) {
            getline(myfile,lineInFile);
            found = lineInFile.find("$$");
            if(found != string::npos) {
                // get the number of CKPT Run
                size_t position = lineInFile.find("CKPT Run ");
                string substring = lineInFile.substr(position+9);
                int CKPT = atoi(substring.c_str());

                // get the number of writer rank
                position = lineInFile.find("writer rank ");
                substring = lineInFile.substr(position+12);
                int writer = atoi(substring.c_str());

                // get the number of elapsed time
                position = lineInFile.find("is ");
                substring = lineInFile.substr(position+3);
                double time = atof(substring.c_str());

                result[CKPT][writer] = time;
            }
        }
    }

    for(int i=0; i<10; i++) {
        for(int j=0; j<130; j++) {
            cout<<result[i][j]<<" ";
        }
        cout<<endl;
    }

    return 0;
}
