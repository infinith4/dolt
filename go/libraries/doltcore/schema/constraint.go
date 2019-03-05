package schema

import "github.com/attic-labs/noms/go/types"

// ColConstraint is an interface used for evaluating whether a columns value is valid
type ColConstraint interface {
	// SatisfiesConstraint takes in a value and returns true if the value satisfies the constraint
	SatisfiesConstraint(value types.Value) bool

	// GetConstraintType returns a string representation of the type of constraint.  This is used for serialization and
	// deserialization of constraints (see ColConstraintFromTypeAndParams).
	GetConstraintType() string

	// GetConstraintParams returns a map[string]string containing the contstraints parameters.  This is used for
	// serialization and deserialization of constraints, and a deserialized constraint must be able to reproduce the same
	// behavior based on the parameters in this map (See ColConstraintFromTypeAndParams).
	GetConstraintParams() map[string]string
}

const (
	NotNullConstraintType = "not_null"
)

// ColConstraintFromTypeAndParams takes in a string representing the type of the constraint and a map of parameters
// that can be used to determine the behavior of the constraint.  An example might be a constraint which validated
// a value is in a given range.  For this the constraint type might by "in_range_constraint", and the parameters might
// be {"min": -10, "max": 10}
func ColConstraintFromTypeAndParams(colCnstType string, params map[string]string) ColConstraint {
	switch colCnstType {
	case NotNullConstraintType:
		return NotNullConstraint{}
	}
	panic("Unknown column constraint type: " + colCnstType)
}

// NotNullConstraint validates that a value is not null.  It does not restrict 0 length strings, or 0 valued ints, or
// anything other than non nil values
type NotNullConstraint struct{}

// SatisfiesConstraint returns true if value is not nil and not types.NullValue
func (nnc NotNullConstraint) SatisfiesConstraint(value types.Value) bool {
	return !types.IsNull(value)
}

// GetConstraintType returns "not_null"
func (nnc NotNullConstraint) GetConstraintType() string {
	return NotNullConstraintType
}

// GetConstraintParams returns nil as this constraint does not require any parameters.
func (nnc NotNullConstraint) GetConstraintParams() map[string]string {
	return nil
}

// ColConstraintsAreEqual validates two ColConstraint slices are identical.
func ColConstraintsAreEqual(a, b []ColConstraint) bool {
	if len(a) != len(b) {
		return false
	} else if len(a) == 0 {
		return true
	}

	// kinda shitty.  Probably shouldn't require order to be identital
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]

		if ca.GetConstraintType() != cb.GetConstraintType() {
			return false
		} else {
			pa := ca.GetConstraintParams()
			pb := cb.GetConstraintParams()

			if len(pa) != len(pb) {
				return false
			} else if len(pa) != 0 {
				for k, va := range pa {
					vb, ok := pb[k]

					if !ok {
						return false
					} else if va != vb {
						return false
					}
				}
			}
		}
	}

	return true
}
