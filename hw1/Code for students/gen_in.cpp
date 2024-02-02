#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <iomanip>

using namespace std;

int main(int argc, char* argv[]){
    /* Input check */
	if (argc < 6){
		cout << "Usage: " << argv[0] << " <k> <n> <c> <t> <input_name>" << endl;
		return -1;
	}

    /* Parse input */
    int k_datacenter_num = atoi(argv[1]);
    int n_service_num = atoi(argv[2]);
    float c_imbalance_tolerance_rate = atof(argv[3]);
    int t_SC_num = atoi(argv[4]);
    string file_name = argv[5];

    /* Check input */
    if (c_imbalance_tolerance_rate < 1){
        cout << "Wrong input configuration: c has to be at least 1" << endl;
        return -1;
    }
    if (c_imbalance_tolerance_rate*n_service_num/k_datacenter_num <= 1){
        // Note: otherwise, the solution is trivial
        cout << "Wrong input configuration: the upper bound of the service number for the datacenter should be greater than 1" << endl;
        return -1;
    }
    if (k_datacenter_num <= 1){
        // Note: otherwise, the solution is trivial
        cout << "Wrong input configuration: k should be greater than 1" << endl;
        return -1;
    }
    if (n_service_num <= k_datacenter_num){
        // Note: otherwise, the solution is trivial
        cout << "Wrong input configuration: n should be greater than k" << endl;
        return -1;
    }
    if (t_SC_num <= 0){
        // Note: otherwise, the solution is trivial
        cout << "Wrong input configuration: t should be greater than 0" << endl;
        return -1;
    }

    /* Pass the checking, start generating the input file */
    /* File check */
	ofstream ofs(file_name + ".in");
	if (!ofs.is_open()){
		cout << "Input file: " << file_name << " cannot be opened." << endl;
		return -1;
	}

    /* write basic iniformation */
    ofs << k_datacenter_num << " "
        << n_service_num << " "
        << c_imbalance_tolerance_rate << " "
        << t_SC_num << endl;

    /* write service chains */
    srand( time(NULL) );
    for (int SC_i = 0; SC_i < t_SC_num; SC_i++){
        double bandwidth = rand() / (RAND_MAX + 1.0)*(100 - 1) + 1;
        ofs << fixed << setprecision(2) << bandwidth << " ";
        int service_num = rand() % 20 + 1;
        ofs << service_num;
        for (int service_i = 0; service_i < service_num; service_i++){
            int service_id = rand() % n_service_num;
            ofs << " " << service_id;
        }
        ofs << endl;
    }

	return 0;
}
