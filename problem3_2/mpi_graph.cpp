#include <mpi.h>
#include <bits/stdc++.h>
using namespace std;   

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc < 2){
        if(rank == 0){
            cout << "Usage: mpirun -np <num_processes> ./mpi_graph <graph_file>" << endl;
        }
        MPI_Finalize();
        return 0;
    }

    char hostname[MPI_MAX_PROCESSOR_NAME];
    int name_len;

    MPI_Get_processor_name(hostname, &name_len);
    cout << "Process " << rank << " running on " << hostname << endl;

    string filename = argv[1];
    int n, m;
    vector<pair<int, int>> edges;

    if(rank == 0){
        ifstream fin(filename);
        fin >> n >> m;
        for(int i = 0; i < m; i++){
            int u, v, w;
            fin >> u >> v >> w;
            edges.push_back({u, v});
        }
    }

    MPI_Bcast(&n, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int edges_per_proc = (rank == 0 ? edges.size() : 0);
    MPI_Bcast(&edges_per_proc, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if(rank != 0){
        edges.resize(edges_per_proc);
    }

    MPI_Bcast(edges.data(), edges_per_proc * 2, MPI_INT, 0, MPI_COMM_WORLD);

    vector<int> comp(n);
    for(int i = 0; i < n; i++){
        comp[i] = i;
    }

    bool changed = true;
    double start_time, end_time;

    MPI_Barrier(MPI_COMM_WORLD);
    start_time = MPI_Wtime();
    
    while(changed){
        changed = false;
        vector<int> new_comp = comp;

        for (auto &e : edges){
            int u = e.first, v = e.second;
            int min_comp = min(comp[u], comp[v]);
            if(new_comp[u] > min_comp){
                new_comp[u] = min_comp;
                changed = true;
            }
            if(new_comp[v] > min_comp){
                new_comp[v] = min_comp;
                changed = true;
            }
        }

        // Reduce to get minimum component ID for each vertex across all processes
        MPI_Allreduce(MPI_IN_PLACE, new_comp.data(), n, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
        
        // Check if any component changed
        bool local_changed = false;
        for(int i = 0; i < n; i++){
            if (comp[i] != new_comp[i]){
                local_changed = true;
                break;
            }
        }
        
        // Update comp with new values
        comp = new_comp;
        
        // Check globally if any process had changes
        MPI_Allreduce(&local_changed, &changed, 1, MPI_CXX_BOOL, MPI_LOR, MPI_COMM_WORLD);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    end_time = MPI_Wtime();

    if (rank == 0) {
        cout << "Total Execution Time: "
             << (end_time - start_time) << " seconds" << endl;
    }

    if(rank == 0){
        for(int i = 0; i < n; i++){
            cout << i << " " << comp[i] << endl;
        }
    }

    MPI_Finalize();
    return 0;
}