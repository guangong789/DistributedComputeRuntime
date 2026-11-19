#include <cuda_runtime.h>

#include <array>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string_view>

constexpr int vector_length = 64;
using Vector = std::array<std::uint32_t, vector_length>;

__global__ void transform_vector(std::uint32_t* values) {
    const unsigned i = threadIdx.x;
    const std::uint32_t n = values[i];
    values[i] = n * n + 3u * i + 11u;
}

bool read_input(Vector& values) {
    constexpr int max_payload = vector_length * 5 + vector_length - 1;
    char line[max_payload + 2];
    if (!std::cin.getline(line, sizeof(line)) || std::cin.eof()) {
        return false;
    }
    std::string_view remaining(line, std::cin.gcount() - 1); // Exclude the LF.
    if (std::cin.peek() != EOF) {
        return false;
    }

    for (int i = 0; i < vector_length; ++i) {
        const auto comma = remaining.find(',');
        if ((comma == std::string_view::npos) != (i == vector_length - 1)) {
            return false;
        }
        const auto part = remaining.substr(0, comma);
        if (part.empty() || part.size() > 5 || (part.size() > 1 && part.front() == '0')) {
            return false;
        }
        const auto end = part.data() + part.size();
        const auto result = std::from_chars(part.data(), end, values[i]);
        if (result.ec != std::errc{} || result.ptr != end || values[i] > 65535) {
            return false;
        }
        if (comma != std::string_view::npos) {
            remaining.remove_prefix(comma + 1);
        }
    }
    return true;
}

bool check(const char* stage, cudaError_t code) {
    std::fprintf(stderr, "%s=%s(%d)\n", stage, cudaGetErrorName(code), static_cast<int>(code));
    return code == cudaSuccess;
}

bool transform(const Vector& input, Vector& output) {
    std::uint32_t* device = nullptr;
    constexpr std::size_t bytes = vector_length * sizeof(std::uint32_t);
    if (!check("cudaMalloc", cudaMalloc(&device, bytes))) {
        return false;
    }
    bool ok = check("host_to_device", cudaMemcpy(device, input.data(), bytes, cudaMemcpyHostToDevice));
    if (ok) {
        transform_vector<<<1, vector_length>>>(device);
        ok = check("kernel_launch", cudaGetLastError())
            && check("cudaDeviceSynchronize", cudaDeviceSynchronize())
            && check("device_to_host", cudaMemcpy(output.data(), device, bytes, cudaMemcpyDeviceToHost));
    }
    const bool freed = check("cudaFree", cudaFree(device));
    return ok && freed;
}

int main() {
    Vector input{}, output{};
    if (!read_input(input)) {
        std::fputs("expected one canonical 64-element input line\n", stderr);
        return 1;
    }
    int count = 0;
    cudaDeviceProp properties{};
    if (!check("cudaGetDeviceCount", cudaGetDeviceCount(&count)) || count < 1
        || !check("cudaGetDeviceProperties", cudaGetDeviceProperties(&properties, 0))
        || !check("cudaSetDevice", cudaSetDevice(0))) {
        return 1;
    }
    std::fprintf(stderr, "device=0 name=%.128s cc=%d.%d uuid=", properties.name, properties.major, properties.minor);
    for (int i = 0; i < 16; ++i) {
        std::fprintf(stderr, "%s%02x", i == 4 || i == 6 || i == 8 || i == 10 ? "-" : "",
                     static_cast<unsigned>(static_cast<unsigned char>(properties.uuid.bytes[i])));
    }
    std::fputc('\n', stderr);

    const bool finished = transform(input, output);
    const bool reset = check("cudaDeviceReset", cudaDeviceReset());
    if (!finished || !reset) {
        return 1;
    }
    for (int i = 0; i < vector_length; ++i) {
        if (std::printf("%s%u", i == 0 ? "" : ",", static_cast<unsigned>(output[i])) < 0) {
            return 1;
        }
    }
    return std::putchar('\n') == EOF || std::fflush(stdout) != 0;
}
