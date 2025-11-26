package ids

import (
	"github.com/google/uuid"
)

// CreateUUID returns a random UUID v4 encoded as a 36-character string.
func CreateUUID() string {
	return uuid.New().String()
}
