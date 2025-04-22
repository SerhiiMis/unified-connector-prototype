#include <fstream>
#include <iostream>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: binary_log_reader <path-to-binfile>\n";
        return 1;
    }
    std::ifstream in(argv[1], std::ios::binary);
    if (!in) {
        std::cerr << "Cannot open file " << argv[1] << "\n";
        return 1;
    }

    std::cout << "[";

    bool first = true;
    while (true) {
        int32_t id;
        float value;
        // read an int32 then a float
        in.read(reinterpret_cast<char*>(&id), sizeof(id));
        if (!in) break;
        in.read(reinterpret_cast<char*>(&value), sizeof(value));
        if (!in) break;

        if (!first) std::cout << ",";
        first = false;
        // print a tiny JSON object
        std::cout << "{"
                  << "\"id\":"   << id   << ","
                  << "\"value\":" << value
                  << "}";
    }

    std::cout << "]";
    return 0;
}
