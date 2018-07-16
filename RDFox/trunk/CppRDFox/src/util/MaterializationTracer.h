// RDFox(c) Copyright University of Oxford, 2013. All Rights Reserved.

#ifndef MATERIALIZATIONTRACER_H_
#define MATERIALIZATIONTRACER_H_

#include "../reasoning/MaterializationMonitor.h"
#include "AbstractTracer.h"

class MaterializationTracer : public AbstractTracer<MaterializationMonitor> {

public:

    MaterializationTracer(Prefixes& prefixes, Dictionary& dictionary, std::ostream& output);

};

#endif /* MATERIALIZATIONTRACER_H_ */
