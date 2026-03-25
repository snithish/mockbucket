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
	S3         S3SeedConfig     `yaml:"s3"`
	GCS        GCSSeedConfig    `yaml:"gcs"`
}

type ObjectSeed struct {
	Bucket  string `yaml:"bucket"`
	Key     string `yaml:"key"`
	Content string `yaml:"content"`
}

type S3SeedConfig struct {
	AccessKeys []S3AccessKeySeed `yaml:"access_keys"`
}

type S3AccessKeySeed struct {
	ID        string `yaml:"id"`
	Secret    string `yaml:"secret"`
	Principal string `yaml:"principal"`
}

type GCSSeedConfig struct {
	Tokens []GCSTokenSeed `yaml:"tokens"`
}

type GCSTokenSeed struct {
	Token     string `yaml:"token"`
	Principal string `yaml:"principal"`
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
				if principal != "*" {
					if _, ok := principalSet[principal]; !ok {
						problems = append(problems, fmt.Sprintf("roles[%d].trust references unknown principal %q", i, principal))
					}
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
	keySet := map[string]struct{}{}
	for i, key := range d.S3.AccessKeys {
		if strings.TrimSpace(key.ID) == "" {
			problems = append(problems, fmt.Sprintf("s3.access_keys[%d].id is required", i))
		}
		if strings.TrimSpace(key.Secret) == "" {
			problems = append(problems, fmt.Sprintf("s3.access_keys[%d].secret is required", i))
		}
		if strings.TrimSpace(key.Principal) == "" {
			problems = append(problems, fmt.Sprintf("s3.access_keys[%d].principal is required", i))
		} else if _, ok := principalSet[key.Principal]; !ok {
			problems = append(problems, fmt.Sprintf("s3.access_keys[%d].principal references unknown principal %q", i, key.Principal))
		}
		if strings.TrimSpace(key.ID) != "" {
			if _, ok := keySet[key.ID]; ok {
				problems = append(problems, fmt.Sprintf("s3.access_keys[%d].id %q is not unique", i, key.ID))
			}
			keySet[key.ID] = struct{}{}
		}
	}
	tokenSet := map[string]struct{}{}
	for i, t := range d.GCS.Tokens {
		if strings.TrimSpace(t.Token) == "" {
			problems = append(problems, fmt.Sprintf("gcs.tokens[%d].token is required", i))
		}
		if strings.TrimSpace(t.Principal) == "" {
			problems = append(problems, fmt.Sprintf("gcs.tokens[%d].principal is required", i))
		} else if _, ok := principalSet[t.Principal]; !ok {
			problems = append(problems, fmt.Sprintf("gcs.tokens[%d].principal references unknown principal %q", i, t.Principal))
		}
		if _, ok := tokenSet[t.Token]; ok {
			problems = append(problems, fmt.Sprintf("gcs.tokens[%d].token %q is not unique", i, t.Token))
		} else {
			tokenSet[t.Token] = struct{}{}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
