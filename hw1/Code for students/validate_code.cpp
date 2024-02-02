#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <ctime>
#include <unistd.h>
#include <sys/resource.h>
#include <cmath>
#include "answer.h"
using namespace std;

// When I grade the homework, I'll use this code to validate your code.
// Please make sure that your code can compile with this code.

int main(int argc, char* argv[]){
    /* Input check */
	if (argc < 2){
		cout << "Usage: " << argv[0] << " <input_file (without .in)>" << endl;
		return -1;
	}
    string file_name = argv[1];

    /* File check */
	ifstream ifs(file_name + ".in");
	if (!ifs.is_open()){
		cout << "Input file: " << file_name+".in" << " cannot be found." << endl;
		return -1;
	}


    /* Run the code */
    const clock_t start_time = clock();
    servie_chain_deployment(file_name);
    double run_time = (double)(clock() - start_time) / CLOCKS_PER_SEC;

    /* Get the memory usage*/
    struct rusage r_usage;
	getrusage(RUSAGE_SELF,&r_usage);


    /* Examine your answer

    /* Parse the input file */
    int k_datacenter_num;
    int n_service_num;
    float c_imbalance_tolerance_rate;
    int t_SC_num;
    ifs >> k_datacenter_num >> n_service_num >> c_imbalance_tolerance_rate >> t_SC_num;
    cout << "Input configuration: k=" << k_datacenter_num << " n="
        << n_service_num << " c="
        << c_imbalance_tolerance_rate << " t="
        << t_SC_num << endl;

    /* Check the output file */
	ifstream ifs_result(file_name + ".out");
	if (!ifs_result.is_open()){
		cout << "Output file: " << file_name+".out" << " cannot be found." << endl;
		return -1;
	}

    /* Validate the result */
    int* service_datacenter_map = new int[n_service_num]();
    // Initial the mapping table
    for (int service_id = 0; service_id < n_service_num; service_id++){
        service_datacenter_map[service_id] = -1; // Not assigned to any datacenter
    }
    for (int datacenter_i = 0; datacenter_i < k_datacenter_num; datacenter_i++){
        int service_num;
        // 用於檢查是否已經到達文件末尾
        if (ifs_result.eof()){
            cout << "Error: Datacenter " << datacenter_i << " is missing" << endl;
            return -1;
        }
        ifs_result >> service_num;

        // Check whether the service number fall between n/ck and cn/k
        if ( service_num < floor(n_service_num/(c_imbalance_tolerance_rate*k_datacenter_num)) ){
            cout << "Error: the service number of Dataceter " << datacenter_i << " is less than the bound n/(ck)" << endl;
            return -1;
        }
        if ( service_num > ceil(c_imbalance_tolerance_rate*n_service_num/k_datacenter_num) ){
            cout << "Error: the service number of Dataceter " << datacenter_i << " is greater than the bound cn/k" << endl;
            return -1;
        }

        for (int service_i = 0; service_i < service_num; service_i++){
            // Assign the service to the datacenter
            int service_id;
            ifs_result >> service_id;
            if (service_datacenter_map[service_id] != -1){
                cout << "Error: the service " << service_id << " is assigned to more than one datacenter" << endl;
                return -1;
            }
            service_datacenter_map[service_id] = datacenter_i;
        }
    }

    // Check if any of the service is not assigned to any datacenter
    for (int service_id = 0; service_id < n_service_num; service_id++){
        if (service_datacenter_map[service_id] == -1){
            cout << "Error: the service " << service_id << " is not assigned to any datacenter" << endl;
            return -1;
        }
    }

    // Calculate the cost
    double total_cost = 0;
    for (int SC_i = 0; SC_i < t_SC_num; SC_i++){
        double bandwidth;
        int service_num;
        int service_id;
        ifs >> bandwidth >> service_num >> service_id;
        // Get the datacenter id of the first service
        int pre_datacenter = service_datacenter_map[service_id];
        service_num--;
        for (int service_i = 0; service_i < service_num; service_i++){
            ifs >> service_id;
            int current_datacenter = service_datacenter_map[service_id];
            // Switch to another datacenter
            if (current_datacenter != pre_datacenter){
                total_cost += bandwidth;
                pre_datacenter = current_datacenter;
            }
        }

    }

    delete [] service_datacenter_map;

    cout << "Your result is valid." << endl;
    cout << "=====  Answer Report  =====" << endl;
	cout << "Deployment Cost: " << total_cost << endl;
    cout << "CPU Run time: " << run_time << " seconds" << endl;
	cout << "Mem Usage:    " << r_usage.ru_maxrss << " k-bytes" << endl;

    ofstream ofs(file_name+"_report.out");
	if (!ofs.is_open()){
		cout << "Output file: " << file_name+"_report.out" << " cannot be opened." << endl;
		return -1;
	}
    ofs << "=====  Answer Report  =====" << endl;
	ofs << "Deployment Cost: " << total_cost << endl;
    ofs << "CPU Run time: " << run_time << endl;
	ofs << "Mem Usage:    " << r_usage.ru_maxrss << endl;

    return 0;
}
