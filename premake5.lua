workspace("FlashLog")
architecture("x86_64")
configurations({ "Debug", "Release" })
startproject("flashlog")

outputdir = "%{cfg.buildcfg}-macos-%{cfg.architecture}"

project("flashlog")
kind("ConsoleApp")
language("C++")
cppdialect("C++20")
staticruntime("off")

targetdir("build/bin/" .. outputdir)
objdir("build/obj/" .. outputdir)

files({ "src/**.h", "src/**.cpp" })
includedirs({ "src" })

filter("system:macosx")
systemversion("latest")
buildoptions({
	"-Wall",
	"-Wextra",
	"-Wpedantic",
	"-Wshadow",
	"-Wconversion",
	"-fno-omit-frame-pointer",
})

filter("configurations:Debug")
symbols("On")
defines({ "FLASHLOG_DEBUG" })

filter("configurations:Release")
optimize("Speed")
defines({ "NDEBUG" })
