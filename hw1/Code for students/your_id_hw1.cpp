#include "answer.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cmath>
#include<algorithm>
#include <unordered_set>
#include<set>
#include <iomanip>
#include <queue>
#define pii std::pair<int,int>
struct  SC
{
    std::set<int> connect_service;
    int service_nums;
};
struct  Pair
{
    std::vector<int> service; // (service1,service2)
    double bandwidth;

    Pair(int& service1, int& service2, double& bw) : service{service1, service2}, bandwidth(bw) {}
};
void SaveToFile(const std::vector<std::set<int>>& matrix, const std::string& filename);
bool CmpCompareBandWidth(const Pair& a,const Pair& b);


// method 1 sort by maximun bandwidth and add service chain into same datacenter
// method2 sort by maximun connect and add into

void servie_chain_deployment(std::string file_name){
    // The input file name does NOT includes ".in"
    // You'll need to handle the file I/O here by yourself
    // Don't forget to write the ".out" file

    // open the file
    std::ifstream inputFile(file_name+".in");
    if(!inputFile.is_open()){
        std::cerr << "can't open " << file_name << std::endl;
        return;
    }

    std::cout << "-------- Start read " << file_name+".in" << " --------"<< std::endl;
    int k_datacenter_num,n_service_num,SC_num;
    int datacenter_upper_bound,datacenter_lowwer_bound,most_upper_bound;
    double c_imbalance_tolerance_rate;
    inputFile >> k_datacenter_num >> n_service_num >> c_imbalance_tolerance_rate >> SC_num;
    datacenter_lowwer_bound=floor(n_service_num/(c_imbalance_tolerance_rate*k_datacenter_num));
    datacenter_upper_bound=ceil(c_imbalance_tolerance_rate*n_service_num/k_datacenter_num);
    most_upper_bound=datacenter_lowwer_bound==datacenter_upper_bound?datacenter_upper_bound:(n_service_num-k_datacenter_num*datacenter_lowwer_bound)/(datacenter_upper_bound-datacenter_lowwer_bound);
    // std::vector<SC> Service_Chain(SC_num);
    std::vector<Pair> ServicePair;
    std::set<int> used_service,unused_service;
    std::vector<std::set<int>> DataCenter(k_datacenter_num);
    std::vector<std::vector<double>> BandWidthRecord(n_service_num,std::vector<double>(n_service_num,0));
    std::vector<int> service_connect_num(n_service_num,0);
    std::vector<int> service_datacenter_map(n_service_num,-1);
    double TotalBandWidth=0;
    std::cout << "datacenter_lowwer_bound : " << datacenter_lowwer_bound  << " | datacenter_upper_bound : " << datacenter_upper_bound << " | most_upper_bound : " << most_upper_bound  <<std::endl;
    // Record all service connect with costing BandWidth
    for (int SC_i = 0; SC_i < SC_num; SC_i++){
        double bandwidth;
        int service_num;
        int service_id;
        inputFile >> bandwidth >> service_num;
        // Service_Chain[SC_i].service_nums=service_num;
        // Service_Chain[SC_i].bandwidth=bandwidth;
        int prev_service_id=-1;
        for (int service_i = 0; service_i < service_num; service_i++){
            inputFile >> service_id;
            // Service_Chain[SC_i].service.push_back(service_id);
            // record the cost bandwidth
            if(prev_service_id!=-1){
                BandWidthRecord[prev_service_id][service_id]+=bandwidth;
                BandWidthRecord[service_id][prev_service_id]+=bandwidth;
            }
            used_service.insert(service_id);
            prev_service_id=service_id;
        }
    }
    // init unused_service
    for(int i=0;i<n_service_num;i++) unused_service.insert(i);
    // find unused_service
    for (auto it = used_service.begin(); it != used_service.end(); ++it) unused_service.erase(*it);
    // construct the service-service connection pair
    for(int service1=0;service1<n_service_num;service1++){
        for(int service2=service1+1;service2<n_service_num;service2++){
            if(BandWidthRecord[service1][service2]==0) continue;
            ServicePair.push_back({service1,service2,BandWidthRecord[service1][service2]});
            service_connect_num[service1]++;
            service_connect_num[service2]++;
            TotalBandWidth+=BandWidthRecord[service1][service2];
        }
    }
    std::cout  << "used service : " << std::setw(3) << used_service.size()  << "  unused_service : " << unused_service.size() << std::endl;
    // print all pair
    // for(int i=0;i<ServicePair.size();i++) std::cout<<std::setw(5) << ServicePair[i].service[0] <<std::setw(5) << ServicePair[i].service[1] << std::setw(5) << ServicePair[i].bandwidth<<std::endl;
    std::string data_line;
    if(std::getline(inputFile, data_line) && data_line.empty())  std::cout << "-------- File read complete --------" << std::endl;
    else std::cout << "-------- File has not been read completely!!!!! --------" << std::endl;
    inputFile.close();
    // end of read file

    // case 1 如果使用過的service 都可以塞在同一個datacenter裡面的話
    if(used_service.size()<=datacenter_upper_bound && unused_service.size()/(k_datacenter_num-1)>=datacenter_lowwer_bound){
        std::cout << "----------- Case1 - less service to do -----------" <<std::endl;
        //assign all datachain in one datacenter
        for (auto it = used_service.begin(); it != used_service.end(); ++it) DataCenter[0].insert(*it);
        used_service.clear();
        // assign unused_service into each datacenter
        std::priority_queue<pii, std::vector<pii>, std::greater<pii>> pq;
        for(int i=0;i<k_datacenter_num;i++) pq.push({DataCenter[i].size(),i});
        // assign remaining unused_service to datacenter
        while(!unused_service.empty()){
            auto it=unused_service.begin();
            auto [size,kidx]=pq.top();pq.pop();
            DataCenter[kidx].insert(*it);
            pq.push({size+1,kidx});
            unused_service.erase(it);
        }
        // save  to .out file
        SaveToFile(DataCenter,file_name);
        return;
    }

    // use Kruskal's Algorithm
    int kidx;
    // sort by bandwidth
    std::sort(ServicePair.begin(),ServicePair.end(),CmpCompareBandWidth);

    std::set<int> empty_datacenter;
    std::vector<pii> node_node;
    for(int i=0;i<k_datacenter_num;i++) empty_datacenter.insert(i);

    // start to do Algorithm
    for(int idx=0;idx<ServicePair.size()  && most_upper_bound!=0;idx++){
        int service1=ServicePair[idx].service[0];
        int service2=ServicePair[idx].service[1];
        // std::cout << std::setw(5) << service1 << std::setw(5) << service2 << std::setw(5) << " " << ServicePair[idx].bandwidth << std::endl;

        if(service_datacenter_map[service1]==-1 && service_datacenter_map[service2]==-1){
            // node-node 都沒有再問一個datacenter裡面
            if(empty_datacenter.empty()){
                node_node.push_back({service1,service2});
                continue;
            }
            auto it=empty_datacenter.begin();
            kidx=*it;
            empty_datacenter.erase(it);
            DataCenter[kidx].insert(service1);
            DataCenter[kidx].insert(service2);
            service_datacenter_map[service1]=kidx;
            service_datacenter_map[service2]=kidx;
            used_service.erase(service1);
            used_service.erase(service2);
            // std::cout << "service : " << service1 << " " << service2 << " into " << kidx << std::endl;
        }else if(service_datacenter_map[service1]==-1 || service_datacenter_map[service2]==-1){ //有一個已經有分配到了
            int kth_datacenter=service_datacenter_map[service1]==-1?service_datacenter_map[service2]:service_datacenter_map[service1];
            int service=service_datacenter_map[service1]==-1?service1:service2;
            if(DataCenter[kth_datacenter].size()+1<=datacenter_upper_bound){
                // std::cout << "service : " << service << " insert into " << kth_datacenter << std::endl;
                DataCenter[kth_datacenter].insert(service);
                service_datacenter_map[service]=kth_datacenter;
                used_service.erase(service);
                if(DataCenter[kth_datacenter].size()==datacenter_upper_bound) most_upper_bound--;
            }
            else continue;
        }else{ // 兩個都有分配到
            // 兩個做合併，合併到service1
            if(service_datacenter_map[service1]!=service_datacenter_map[service2] && (DataCenter[service_datacenter_map[service1]].size()+DataCenter[service_datacenter_map[service2]].size())<=datacenter_upper_bound){
                std::set<int> unionset;
                std::set_union(DataCenter[service_datacenter_map[service1]].begin(),DataCenter[service_datacenter_map[service1]].end(),
                               DataCenter[service_datacenter_map[service2]].begin(),DataCenter[service_datacenter_map[service2]].end(),
                               std::inserter(unionset,unionset.begin()));

                // std::cout <<  "Combine : " <<  service_datacenter_map[service1] << " with " << service_datacenter_map[service2] << std::endl;

                int remove_datacenter=service_datacenter_map[service2];
                empty_datacenter.insert(remove_datacenter);
                // 把原本的map更改掉
                for(int n:DataCenter[remove_datacenter]) service_datacenter_map[n]=service_datacenter_map[service1];
                DataCenter[remove_datacenter].clear();
                if(!DataCenter[remove_datacenter].empty()) std:: cout << "!!! Error datacenter : " << service_datacenter_map[service2] << " is not clear " << std::endl;
                DataCenter[service_datacenter_map[service1]].clear();
                if(!DataCenter[service_datacenter_map[service1]].empty()) std:: cout << "!!! Error datacenter : " << service_datacenter_map[service1] << " is not clear " << std::endl;
                DataCenter[service_datacenter_map[service1]]=unionset;
            }
        }
    }
    std::cout << "--- Stage 1  finished ---"<< std::endl <<  "Remain used-service have " << used_service.size() << " | node-node size is " << node_node.size() <<std::endl;

    // -------------debug
    // for(int i=0;i<k_datacenter_num;i++){
    //     if(DataCenter[i].find(201)!=DataCenter[i].end()) std::cout << "Find in DataCenter" << std::endl;
    // }
    // if(used_service.find(201)!=used_service.end()) std::cout<< "Find in used_service" << std::endl;
    // -----------------

    std::cout << "--- Stage 2  start , Assign  node_node service to datacenter"  << std::endl;
    // assign unused to small than lowwer bound datacenter
    std::priority_queue<pii, std::vector<pii>, std::greater<pii>> pq;
    for(int i=0;i<k_datacenter_num;i++) pq.push({DataCenter[i].size(),i});
    for(int i=0;i<node_node.size();i++){
        auto [size,kidx]=pq.top();
        if(size+2<=datacenter_upper_bound){
            pq.pop();
            if(used_service.find(node_node[i].first)!=used_service.end()){
                DataCenter[kidx].insert(node_node[i].first);
                used_service.erase(node_node[i].first);
                size+=1;
            }
            if(used_service.find(node_node[i].second)!=used_service.end()){
                DataCenter[kidx].insert(node_node[i].second);
                used_service.erase(node_node[i].second);
                size+=1;
            }
            pq.push({size,kidx});
        }else{
            break;
        }
    }
    std::cout << "--- Stage 2  finished --- "  << std::endl;

    std::cout << "--- Stage 3  start , Assign  Remaining service to datacenter"  << std::endl;
    // assign remaining used_service to datacenter
    while(!used_service.empty()){
        auto it=used_service.begin();
        auto [size,kidx]=pq.top();pq.pop();
        DataCenter[kidx].insert(*it);
        pq.push({size+1,kidx});
        used_service.erase(it);
    }
    // assign remaining unused_service to datacenter
    while(!unused_service.empty()){
        auto it=unused_service.begin();
        auto [size,kidx]=pq.top();pq.pop();
        DataCenter[kidx].insert(*it);
        pq.push({size+1,kidx});
        unused_service.erase(it);
    }
    std::cout << "--- Stage 3  finished --- "  << std::endl << std::endl;
    std::cout << "--- TotalBandWidth : "  << TotalBandWidth << std::endl ;

    SaveToFile(DataCenter,file_name);
	return;
}

bool CmpCompareBandWidth(const Pair& a,const Pair& b){
    return a.bandwidth>b.bandwidth;
}

void SaveToFile(const std::vector<std::set<int>>& matrix, const std::string& filename) {
    std::ofstream outFile(filename+".out");
    if (!outFile.is_open()) {
        std::cerr << "Output file: " << filename+".out" << " cannot be opened." << std::endl;
		return;
    }
    for (const auto& row : matrix) {
        outFile << row.size() << " ";
        for (int value : row) {
            outFile << value << " ";
        }
        outFile << std::endl;
    }
    outFile.close();
}