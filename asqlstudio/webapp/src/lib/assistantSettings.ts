export type AssistantLLMPreferences = {
  enabled: boolean
  provider: string
  base_url: string
  model: string
  allow_fallback: boolean
}

const STORAGE_KEY = 'asql-assistant-llm-settings-v1'

const DEFAULT_SETTINGS: AssistantLLMPreferences = {
  enabled: false,
  provider: '',
  base_url: '',
  model: '',
  allow_fallback: true,
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
      base_url: typeof parsed.base_url === 'string' ? parsed.base_url.trim() : DEFAULT_SETTINGS.base_url,
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
