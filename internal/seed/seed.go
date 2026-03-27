package seed

import (
	"errors"
	"fmt"
	"strings"
)

type Document struct {
	Buckets []string      `yaml:"buckets"`
	Roles   []RoleSeed    `yaml:"roles"`
	Objects []ObjectSeed  `yaml:"objects"`
	S3      S3SeedConfig  `yaml:"s3"`
	GCS     GCSSeedConfig `yaml:"gcs"`
}

type RoleSeed struct {
	Name string `yaml:"name"`
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
	ID           string   `yaml:"id"`
	Secret       string   `yaml:"secret"`
	AllowedRoles []string `yaml:"allowed_roles"`
}

type GCSSeedConfig struct {
	Tokens             []GCSTokenSeed       `yaml:"tokens"`
	ServiceCredentials []GCSServiceCredSeed `yaml:"service_credentials"`
}

type GCSTokenSeed struct {
	Token     string `yaml:"token"`
	Principal string `yaml:"principal"`
}

type GCSServiceCredSeed struct {
	ClientEmail string `yaml:"client_email"`
	Principal   string `yaml:"principal"`
}

func (d Document) Validate() error {
	var problems []string
	bucketSet := map[string]struct{}{}
	roleSet := map[string]struct{}{}

	for i, bucket := range d.Buckets {
		if strings.TrimSpace(bucket) == "" {
			problems = append(problems, fmt.Sprintf("buckets[%d] is required", i))
			continue
		}
		bucketSet[bucket] = struct{}{}
	}
	for i, role := range d.Roles {
		if strings.TrimSpace(role.Name) == "" {
			problems = append(problems, fmt.Sprintf("roles[%d].name is required", i))
			continue
		}
		roleSet[role.Name] = struct{}{}
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
		for j, role := range key.AllowedRoles {
			if _, ok := roleSet[role]; !ok {
				problems = append(problems, fmt.Sprintf("s3.access_keys[%d].allowed_roles[%d] references unknown role %q", i, j, role))
			}
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
		}
		if _, ok := tokenSet[t.Token]; ok {
			problems = append(problems, fmt.Sprintf("gcs.tokens[%d].token %q is not unique", i, t.Token))
		} else {
			tokenSet[t.Token] = struct{}{}
		}
	}
	emailSet := map[string]struct{}{}
	for i, sc := range d.GCS.ServiceCredentials {
		if strings.TrimSpace(sc.ClientEmail) == "" {
			problems = append(problems, fmt.Sprintf("gcs.service_credentials[%d].client_email is required", i))
		}
		if strings.TrimSpace(sc.Principal) == "" {
			problems = append(problems, fmt.Sprintf("gcs.service_credentials[%d].principal is required", i))
		}
		if _, ok := emailSet[sc.ClientEmail]; ok {
			problems = append(problems, fmt.Sprintf("gcs.service_credentials[%d].client_email %q is not unique", i, sc.ClientEmail))
		} else {
			emailSet[sc.ClientEmail] = struct{}{}
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
