#include <iostream>

#include "signature.h"

int main(int argc, char* argv[]) {
    auto params = signature::ParseArguments(argc, argv);
    if (!params.is_success) {
        std::cerr << params.error_message << '\n';
        return -1;
    }
    signature::CalculateSignature(params);
    std::cout << "Success\n";
}
