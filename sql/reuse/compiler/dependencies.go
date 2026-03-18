package compiler

func (qd *QueryDecomposer) buildDependencies(plan *QueryPlan) {
	// Enhanced dependency building
	for _, comp := range plan.Components {
		if len(comp.Dependencies) > 0 {
			plan.Dependencies[comp.Signature] = comp.Dependencies
		}
	}

	// Build logical dependencies between component types
	var fromSig, whereSig string

	for _, comp := range plan.Components {
		switch comp.Type {
		case FROM_COMPONENT:
			fromSig = comp.Signature
		case WHERE_COMPONENT:
			whereSig = comp.Signature
		}
	}

	// WHERE depends on FROM
	if fromSig != "" && whereSig != "" {
		if plan.Dependencies[whereSig] == nil {
			plan.Dependencies[whereSig] = []string{}
		}
		plan.Dependencies[whereSig] = append(plan.Dependencies[whereSig], fromSig)
	}
}
