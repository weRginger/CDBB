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
	
	// x is CKPT Run; y is writer rank
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
	
	// store the summation of one application in one CKPT run
	// x is CKPT Run; y is application number
	double sum[5][5];
	for(int i=0; i<5; i++) {
		for(int j=0; j<5; j++) {
			sum[i][j] = 0.0;
		}
	}
	
	// store the max value for one application in one CKPT run
	// x is CKPT Run; y is application number
	double max[5][5];
	for(int i=0; i<5; i++) {
		for(int j=0; j<5; j++) {
			max[i][j] = -1.0;
		}
	}

    for(int i=0; i<5; i++) {
        for(int j=0; j<368; j++) {
			
			//cout<<result[i][j]<<" ";
			
            if(j == 0) {
                continue;
            }
			else if(j % 8 == 7) {
				continue;
			}
			else if(j >=1 && j <= 73) {
				sum[i][0] += result[i][j];
				if(result[i][j] > max[i][0]) max[i][0] = result[i][j];
			}
			else if(j >=74 && j <= 146) {
				sum[i][1] += result[i][j];
				if(result[i][j] > max[i][1]) max[i][1] = result[i][j];
			}
			else if(j >=147 && j <= 219) {
				sum[i][2] += result[i][j];
				if(result[i][j] > max[i][2]) max[i][2] = result[i][j];
			}
			else if(j >=220 && j <= 292) {
				sum[i][3] += result[i][j];
				if(result[i][j] > max[i][3]) max[i][3] = result[i][j];
			}
			else if(j >=293 && j <= 365) {
				sum[i][4] += result[i][j];
				if(result[i][j] > max[i][4]) max[i][4] = result[i][j];
			}
			else {
				continue;
			}
        }
        //cout<<endl;
    }
	
	for(int i=0; i<5; i++) {
		for(int j=0; j<5; j++) {
			cout<<max[i][j]<<" ";
		}
		cout<<endl;
	}

    return 0;
}
