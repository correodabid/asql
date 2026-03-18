package studioapp

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

const (
	assistantLLMTransportHTTPJSON = "http-json"

	assistantLLMAPIKeyModeRequired = "required"
	assistantLLMAPIKeyModeOptional = "optional"
	assistantLLMAPIKeyModeNone     = "none"
)

//go:embed assistant_llm_catalog.json
var assistantLLMCatalogFS embed.FS

var (
	assistantLLMCatalogOnce sync.Once
	assistantLLMCatalogData assistantLLMCatalog
	assistantLLMCatalogErr  error
)

type assistantLLMModelCatalog struct {
	ID    string `json:"id"`
	Label string `json:"label,omitempty"`
}

type assistantLLMTransportCatalog struct {
	Type              string            `json:"type"`
	Method            string            `json:"method,omitempty"`
	Path              string            `json:"path,omitempty"`
	Headers           map[string]string `json:"headers,omitempty"`
	Body              interface{}       `json:"body,omitempty"`
	ResponseTextPaths []string          `json:"response_text_paths,omitempty"`
}

type assistantLLMProviderCatalog struct {
	ID                    string                       `json:"id"`
	Label                 string                       `json:"label"`
	Transport             assistantLLMTransportCatalog `json:"transport"`
	DefaultBaseURL        string                       `json:"default_base_url,omitempty"`
	SupportsCustomBaseURL bool                         `json:"supports_custom_base_url,omitempty"`
	SupportsCustomModel   bool                         `json:"supports_custom_model,omitempty"`
	ModelPlaceholder      string                       `json:"model_placeholder,omitempty"`
	APIKeyMode            string                       `json:"api_key_mode,omitempty"`
	APIKeyLabel           string                       `json:"api_key_label,omitempty"`
	APIKeyPlaceholder     string                       `json:"api_key_placeholder,omitempty"`
	Models                []assistantLLMModelCatalog   `json:"models,omitempty"`
}

type assistantLLMCatalog struct {
	DefaultProvider string                        `json:"default_provider"`
	Providers       []assistantLLMProviderCatalog `json:"providers"`
}

func (a *App) AssistantLLMCatalog() (assistantLLMCatalog, error) {
	catalog, err := loadAssistantLLMCatalog()
	if err != nil {
		return assistantLLMCatalog{}, err
	}
	return *catalog, nil
}

func loadAssistantLLMCatalog() (*assistantLLMCatalog, error) {
	assistantLLMCatalogOnce.Do(func() {
		raw, err := assistantLLMCatalogFS.ReadFile("assistant_llm_catalog.json")
		if err != nil {
			assistantLLMCatalogErr = fmt.Errorf("read assistant llm catalog: %w", err)
			return
		}
		var catalog assistantLLMCatalog
		if err := json.Unmarshal(raw, &catalog); err != nil {
			assistantLLMCatalogErr = fmt.Errorf("parse assistant llm catalog: %w", err)
			return
		}
		if err := catalog.validate(); err != nil {
			assistantLLMCatalogErr = err
			return
		}
		assistantLLMCatalogData = catalog
	})
	if assistantLLMCatalogErr != nil {
		return nil, assistantLLMCatalogErr
	}
	catalog := assistantLLMCatalogData
	return &catalog, nil
}

func (c *assistantLLMCatalog) validate() error {
	c.DefaultProvider = strings.ToLower(strings.TrimSpace(c.DefaultProvider))
	if strings.TrimSpace(c.DefaultProvider) == "" {
		return fmt.Errorf("assistant llm catalog default_provider is required")
	}
	if len(c.Providers) == 0 {
		return fmt.Errorf("assistant llm catalog must declare at least one provider")
	}
	seenProviders := make(map[string]struct{}, len(c.Providers))
	defaultFound := false
	for i := range c.Providers {
		provider := &c.Providers[i]
		provider.ID = strings.ToLower(strings.TrimSpace(provider.ID))
		provider.Label = strings.TrimSpace(provider.Label)
		provider.Transport.Type = strings.ToLower(strings.TrimSpace(provider.Transport.Type))
		provider.Transport.Method = strings.ToUpper(strings.TrimSpace(provider.Transport.Method))
		provider.Transport.Path = strings.TrimSpace(provider.Transport.Path)
		provider.DefaultBaseURL = strings.TrimSpace(provider.DefaultBaseURL)
		provider.ModelPlaceholder = strings.TrimSpace(provider.ModelPlaceholder)
		provider.APIKeyMode = strings.ToLower(strings.TrimSpace(provider.APIKeyMode))
		provider.APIKeyLabel = strings.TrimSpace(provider.APIKeyLabel)
		provider.APIKeyPlaceholder = strings.TrimSpace(provider.APIKeyPlaceholder)
		if provider.ID == "" {
			return fmt.Errorf("assistant llm catalog provider id is required")
		}
		if provider.Label == "" {
			provider.Label = provider.ID
		}
		if _, exists := seenProviders[provider.ID]; exists {
			return fmt.Errorf("assistant llm catalog provider %q is duplicated", provider.ID)
		}
		seenProviders[provider.ID] = struct{}{}
		if provider.ID == c.DefaultProvider {
			defaultFound = true
		}
		switch provider.Transport.Type {
		case assistantLLMTransportHTTPJSON:
			if provider.Transport.Method == "" {
				provider.Transport.Method = "POST"
			}
			if provider.Transport.Path == "" {
				return fmt.Errorf("assistant llm catalog provider %q must declare transport.path", provider.ID)
			}
			if len(provider.Transport.ResponseTextPaths) == 0 {
				return fmt.Errorf("assistant llm catalog provider %q must declare at least one response_text_path", provider.ID)
			}
			for key, value := range provider.Transport.Headers {
				trimmedKey := strings.TrimSpace(key)
				trimmedValue := strings.TrimSpace(value)
				if trimmedKey == "" {
					return fmt.Errorf("assistant llm catalog provider %q declares an empty transport header name", provider.ID)
				}
				delete(provider.Transport.Headers, key)
				provider.Transport.Headers[trimmedKey] = trimmedValue
			}
			for idx, path := range provider.Transport.ResponseTextPaths {
				provider.Transport.ResponseTextPaths[idx] = strings.TrimSpace(path)
				if provider.Transport.ResponseTextPaths[idx] == "" {
					return fmt.Errorf("assistant llm catalog provider %q contains an empty response_text_path", provider.ID)
				}
			}
		default:
			return fmt.Errorf("assistant llm catalog provider %q uses unsupported transport type %q", provider.ID, provider.Transport.Type)
		}
		switch provider.APIKeyMode {
		case "":
			provider.APIKeyMode = assistantLLMAPIKeyModeOptional
		case assistantLLMAPIKeyModeRequired, assistantLLMAPIKeyModeOptional, assistantLLMAPIKeyModeNone:
		default:
			return fmt.Errorf("assistant llm catalog provider %q uses unsupported api_key_mode %q", provider.ID, provider.APIKeyMode)
		}
		for j := range provider.Models {
			provider.Models[j].ID = strings.TrimSpace(provider.Models[j].ID)
			provider.Models[j].Label = strings.TrimSpace(provider.Models[j].Label)
			if provider.Models[j].ID == "" {
				return fmt.Errorf("assistant llm catalog provider %q contains a model without id", provider.ID)
			}
			if provider.Models[j].Label == "" {
				provider.Models[j].Label = provider.Models[j].ID
			}
		}
	}
	if !defaultFound {
		return fmt.Errorf("assistant llm catalog default provider %q is not declared", c.DefaultProvider)
	}
	return nil
}

func (c assistantLLMCatalog) providerByID(id string) (assistantLLMProviderCatalog, bool) {
	needle := strings.ToLower(strings.TrimSpace(id))
	for _, provider := range c.Providers {
		if provider.ID == needle {
			return provider, true
		}
	}
	return assistantLLMProviderCatalog{}, false
}
