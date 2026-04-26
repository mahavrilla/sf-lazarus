package iceberg

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type RemovedFieldPolicy string

const (
	PolicyTombstone RemovedFieldPolicy = "tombstone"
	PolicyHalt      RemovedFieldPolicy = "halt"
)

// PartitionConfig declares one partition field and its Iceberg transform.
// Supported transforms: "identity" (default), "day", "hour", "month", "year".
type PartitionConfig struct {
	Field     string `yaml:"field"`
	Transform string `yaml:"transform"` // default: "identity"
}

type ObjectConfig struct {
	Name               string             `yaml:"name"`
	PrimaryKey         string             `yaml:"primary_key"`
	WatermarkField     string             `yaml:"watermark_field"`
	PartitionBy        []PartitionConfig  `yaml:"partition_by"`
	Overflow           bool               `yaml:"overflow"`
	RemovedFieldPolicy RemovedFieldPolicy `yaml:"removed_field_policy"`
	ExcludeFields      []string           `yaml:"exclude_fields"`
}

type Registry struct {
	objects []ObjectConfig
}

func LoadRegistry(path string) (*Registry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read objects yaml: %w", err)
	}

	var cfg struct {
		Objects []ObjectConfig `yaml:"objects"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse objects yaml: %w", err)
	}

	return &Registry{objects: cfg.Objects}, nil
}

// NewRegistry constructs a Registry directly from a slice of ObjectConfigs.
// Intended for tests and programmatic wiring; prefer LoadRegistry for
// production use.
func NewRegistry(objects []ObjectConfig) *Registry {
	out := make([]ObjectConfig, len(objects))
	copy(out, objects)
	return &Registry{objects: out}
}

func (r *Registry) All() []ObjectConfig {
	out := make([]ObjectConfig, len(r.objects))
	copy(out, r.objects)
	return out
}

func (r *Registry) Get(name string) (ObjectConfig, bool) {
	for _, o := range r.objects {
		if o.Name == name {
			return o, true
		}
	}
	return ObjectConfig{}, false
}
