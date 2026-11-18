import os
from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import get, copy

class NatsCConan(ConanFile):
    name = "nats-c"
    license = "Apache-2.0"
    homepage = "https://github.com/nats-io/nats.c"
    package_type = "static-library"
    settings = "os", "arch", "compiler", "build_type"
    def requirements(self):
        self.requires("openssl/[>=1.1 <4]")
    def layout(self):
        cmake_layout(self, src_folder="src")
    def source(self):
        base = (os.getenv("DEPENDENCY_GITHUB_RAW_URL") or "https://github.com").rstrip("/")
        get(self, url=f"{base}/nats-io/nats.c/archive/refs/tags/v{self.version}.tar.gz",
            sha256=self.conan_data["sources"][str(self.version)]["sha256"], strip_root=True)
    def generate(self):
        CMakeDeps(self).generate()
        tc = CMakeToolchain(self)
        for name, value in {"NATS_BUILD_WITH_TLS": True, "NATS_BUILD_STREAMING": False,
                            "NATS_BUILD_EXAMPLES": False, "NATS_BUILD_LIB_SHARED": False,
                            "NATS_BUILD_LIB_STATIC": True, "BUILD_TESTING": False,
                            "CMAKE_POSITION_INDEPENDENT_CODE": True}.items():
            tc.variables[name] = value
        tc.generate()
    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
    def package(self):
        CMake(self).install()
        copy(self, "LICENSE", self.source_folder, os.path.join(self.package_folder, "licenses"))
    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "cnats")
        self.cpp_info.set_property("cmake_target_name", "cnats::nats_static")
        self.cpp_info.libs = ["nats_staticd" if str(self.settings.build_type) == "Debug" else "nats_static"]
        if self.settings.os == "Linux":
            self.cpp_info.system_libs = ["pthread"]
