// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#include "AbstractTracerImpl.h"
#include "MaterializationTracer.h"

MaterializationTracer::MaterializationTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output) : AbstractTracer<MaterializationMonitor>(prefixes, dictionary, output) {
}
