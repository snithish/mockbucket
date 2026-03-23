package seed

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/snithish/mockbucket/internal/core"
)

type Document struct {
	Buckets    []string         `yaml:"buckets"`
	Principals []core.Principal `yaml:"principals"`
	Roles      []core.Role      `yaml:"roles"`
	Objects    []ObjectSeed     `yaml:"objects"`
}

type ObjectSeed struct {
	Bucket  string `yaml:"bucket"`
	Key     string `yaml:"key"`
	Content string `yaml:"content"`
}

func Load(path string) (Document, error) {
	if strings.TrimSpace(path) == "" {
		return Document{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Document{}, fmt.Errorf("read seed: %w", err)
	}
	var doc Document
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return Document{}, fmt.Errorf("parse seed: %w", err)
	}
	if err := doc.Validate(); err != nil {
		return Document{}, err
	}
	return doc, nil
}

func (d Document) Validate() error {
	var problems []string
	bucketSet := map[string]struct{}{}
	principalSet := map[string]struct{}{}
	for i, bucket := range d.Buckets {
		if strings.TrimSpace(bucket) == "" {
			problems = append(problems, fmt.Sprintf("buckets[%d] is required", i))
			continue
		}
		bucketSet[bucket] = struct{}{}
	}
	for i, principal := range d.Principals {
		if strings.TrimSpace(principal.Name) == "" {
			problems = append(problems, fmt.Sprintf("principals[%d].name is required", i))
			continue
		}
		principalSet[principal.Name] = struct{}{}
		for j, key := range principal.AccessKeys {
			if strings.TrimSpace(key.ID) == "" || strings.TrimSpace(key.Secret) == "" {
				problems = append(problems, fmt.Sprintf("principals[%d].access_keys[%d] requires id and secret", i, j))
			}
		}
	}
	for i, role := range d.Roles {
		if strings.TrimSpace(role.Name) == "" {
			problems = append(problems, fmt.Sprintf("roles[%d].name is required", i))
			continue
		}
		for j, statement := range role.Trust.Statements {
			if len(statement.Principals) == 0 {
				problems = append(problems, fmt.Sprintf("roles[%d].trust.statements[%d] requires principals", i, j))
			}
			for _, principal := range statement.Principals {
				if _, ok := principalSet[principal]; !ok {
					problems = append(problems, fmt.Sprintf("roles[%d].trust references unknown principal %q", i, principal))
				}
			}
		}
	}
	for i, object := range d.Objects {
		if _, ok := bucketSet[object.Bucket]; !ok {
			problems = append(problems, fmt.Sprintf("objects[%d].bucket references unknown bucket %q", i, object.Bucket))
		}
		if strings.TrimSpace(object.Key) == "" {
			problems = append(problems, fmt.Sprintf("objects[%d].key is required", i))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
