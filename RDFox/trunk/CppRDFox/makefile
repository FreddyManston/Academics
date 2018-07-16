PROJECT      = CppRDFox
BUILDROOTDIR = build
SRCS         = $(shell find src -type f -name '*.cpp')
CPPTESTDIR   = ../CppTest
UNAME        = $(shell uname)
SVN_REVISION = $(shell svn info | grep "Revision" | sed "s:Revision\: ::")

ifeq ($(UNAME),Darwin)
	TARGET_OS = darwin
else ifeq ($(UNAME),SunOS)
	TARGET_OS  = solaris
	LDLIBS    += -lsocket -lnsl -lmtmalloc -lcurl
	LDFLAGS   += -static-libstdc++
else
	TARGET_OS  = linux
endif

ifeq ($(mode),debug)
	CPPFLAGS += -DDEBUG
	CXXFLAGS += -g -O0
else
    mode      = release
	CPPFLAGS += -DNDEBUG
	CXXFLAGS += -O3
endif

CPPFLAGS += -m64 -pthread -DEXPORT_API -DSVN_REVISION=$(SVN_REVISION) -DDLIB_NO_GUI_SUPPORT -I"external/dlib-18.17" -I"external/metis-5.1.0/include" -I"external/jdk1.8.0/include" -I"external/jdk1.8.0/include/$(TARGET_OS)"
CXXFLAGS += -Wall -std=c++11

LDLIBS  += -lpthread -lmetis
LDFLAGS += -m64 -L"external/metis-5.1.0/lib/$(TARGET_OS)"

ifeq ($(test),yes)
    TESTPART   = -test
	TESTS      = $(shell find test -type f -name '*.cpp')
	CPPFLAGS  += -I"$(CPPTESTDIR)/src" -DWITH_TEST
	LDLIBS    += -lCppTest
	LDFLAGS   += -L"$(CPPTESTDIR)/build/$(mode)"
endif

ifeq ($(apilogging),yes)
    LOGAPI_PART  = -logAPI
	CPPFLAGS  	+= -DAPILOGGING
endif

ifeq ($(dll),yes)
	DLLPART  = -dll
	CPPFLAGS += -fPIC
	ifeq ($(UNAME),Darwin)
		PRODUCT  = lib$(PROJECT)$(LOGAPI_PART).dylib
		LDFLAGS  += -dynamiclib
	else
		PRODUCT = lib$(PROJECT)$(LOGAPI_PART).so
		LDFLAGS += -shared
	endif
else
	PRODUCT = $(PROJECT)$(LOGAPI_PART)
endif

BUILDDIR = $(BUILDROOTDIR)/$(mode)$(TESTPART)$(LOGAPI_PART)$(DLLPART)
OBJS     = $(addprefix $(BUILDDIR)/,$(SRCS:.cpp=.o)) $(addprefix $(BUILDDIR)/,$(TESTS:.cpp=.o))

.PHONY: all
all: product

.PHONY: clean
clean:
	@rm -rf $(BUILDDIR)

.PHONY: cleanall
cleanall:
	@rm -rf $(BUILDROOTDIR)

.PHONY: product
product: $(BUILDDIR)/$(PRODUCT)

$(BUILDDIR)/%.d: ;

$(BUILDDIR)/%.o: %.cpp $(BUILDDIR)/%.d
	@mkdir -p $(dir $@)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -MF"$(@:.o=.d)" -c -o $@ $<

$(BUILDDIR)/$(PRODUCT): $(OBJS)
	$(CXX) $(LDFLAGS) -o $(BUILDDIR)/$(PRODUCT) $(OBJS) $(LDLIBS)

-include $(addprefix $(BUILDDIR)/,$(SRCS:.cpp=.d))
