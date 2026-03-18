export type AssistantLLMPreferences = {
  enabled: boolean
  provider: 'ollama' | 'openai' | string
  base_url: string
  model: string
  allow_fallback: boolean
}

const STORAGE_KEY = 'asql-assistant-llm-settings-v1'

const DEFAULT_SETTINGS: AssistantLLMPreferences = {
  enabled: false,
  provider: 'ollama',
  base_url: 'http://127.0.0.1:11434',
  model: '',
  allow_fallback: true,
}

export function defaultAssistantBaseURL(provider: string) {
  return provider === 'openai' ? 'https://api.openai.com/v1' : 'http://127.0.0.1:11434'
}

export function readAssistantLLMPreferences(): AssistantLLMPreferences {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return DEFAULT_SETTINGS
    const parsed = JSON.parse(raw) as Partial<AssistantLLMPreferences>
    const provider = typeof parsed.provider === 'string' && parsed.provider.trim() ? parsed.provider.trim() : DEFAULT_SETTINGS.provider
    return {
      enabled: parsed.enabled === true,
      provider,
      base_url: typeof parsed.base_url === 'string' && parsed.base_url.trim() ? parsed.base_url.trim() : defaultAssistantBaseURL(provider),
      model: typeof parsed.model === 'string' ? parsed.model.trim() : '',
      allow_fallback: parsed.allow_fallback !== false,
    }
  } catch {
    return DEFAULT_SETTINGS
  }
}

export function writeAssistantLLMPreferences(next: AssistantLLMPreferences) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
  } catch {
    // ignore storage failures
  }
}
