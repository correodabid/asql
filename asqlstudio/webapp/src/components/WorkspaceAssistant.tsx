import { useEffect, useMemo, useState } from 'react'
import { api } from '../lib/api'
import { readAssistantLLMPreferences, writeAssistantLLMPreferences } from '../lib/assistantSettings'
import type { AssistantChatMessage, AssistantLLMCatalog, AssistantLLMProviderOption, AssistantLLMRequest, AssistantQueryPlan, AssistantQueryRequest } from '../types/workspace'
import { IconAlertTriangle, IconChevronDown, IconCode, IconCpu, IconKey, IconPlay, IconX } from './Icons'

type Props = {
  domain: string
  busy: boolean
  onInsertSQL: (sql: string) => void
  onRunSQL: (sql: string, primaryTable?: string) => void
  onClose: () => void
}

const CUSTOM_MODEL_SENTINEL = '__custom_model__'
const AUTO_FIX_PROMPT = 'Fix the previous SQL using the ASQL validation error and keep the original intent.'

type AssistantThreadMessage =
  | { id: string; role: 'user'; content: string }
  | { id: string; role: 'assistant'; plan: AssistantQueryPlan }
  | { id: string; role: 'error'; content: string }

function findProvider(catalog: AssistantLLMCatalog | null, providerId: string): AssistantLLMProviderOption | null {
  if (!catalog) return null
  return catalog.providers.find((provider) => provider.id === providerId) ?? null
}

function fallbackProvider(catalog: AssistantLLMCatalog | null): AssistantLLMProviderOption | null {
  if (!catalog || catalog.providers.length === 0) return null
  return findProvider(catalog, catalog.default_provider) ?? catalog.providers[0] ?? null
}

export function WorkspaceAssistant({ domain, busy, onInsertSQL, onRunSQL, onClose }: Props) {
  const initialLLM = readAssistantLLMPreferences()
  const [question, setQuestion] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [messages, setMessages] = useState<AssistantThreadMessage[]>([])
  const [catalog, setCatalog] = useState<AssistantLLMCatalog | null>(null)
  const [catalogError, setCatalogError] = useState('')
  const [useLLM, setUseLLM] = useState(initialLLM.enabled)
  const [provider, setProvider] = useState(initialLLM.provider)
  const [baseURL, setBaseURL] = useState(initialLLM.base_url)
  const [model, setModel] = useState(initialLLM.model)
  const [apiKey, setAPIKey] = useState(initialLLM.api_key)
  const [allowFallback, setAllowFallback] = useState(initialLLM.allow_fallback)
  const [showConfig, setShowConfig] = useState(false)
  const [customModel, setCustomModel] = useState(false)

  const activeProvider = useMemo(() => findProvider(catalog, provider) ?? fallbackProvider(catalog), [catalog, provider])
  const apiKeyRequired = activeProvider?.api_key_mode === 'required'
  const activeProviderModels = activeProvider?.models ?? []
  const selectedModelValue = customModel ? CUSTOM_MODEL_SENTINEL : model
  const latestAssistantPlan = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const message = messages[i]
      if (message.role === 'assistant') {
        return message.plan
      }
    }
    return null
  }, [messages])

  useEffect(() => {
    setMessages([])
    setError('')
    setQuestion('')
  }, [domain])

  useEffect(() => {
    let cancelled = false
    void api<AssistantLLMCatalog>('/api/assistant/catalog')
      .then((response) => {
        if (cancelled) return
        setCatalog(response)
        setCatalogError('')
      })
      .catch((err) => {
        if (cancelled) return
        setCatalog(null)
        setCatalogError(err instanceof Error ? err.message : String(err))
      })
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (!catalog) return
    const nextProvider = findProvider(catalog, provider) ?? fallbackProvider(catalog)
    if (!nextProvider) return
    if (provider !== nextProvider.id) {
      setProvider(nextProvider.id)
      if (!baseURL.trim()) {
        setBaseURL(nextProvider.default_base_url ?? '')
      }
      return
    }
    if (!baseURL.trim() && nextProvider.default_base_url) {
      setBaseURL(nextProvider.default_base_url)
    }
  }, [baseURL, catalog, provider])

  useEffect(() => {
    if (!activeProvider) return
    if (activeProviderModels.length === 0) {
      setCustomModel(true)
      return
    }
    const hasKnownModel = activeProviderModels.some((option) => option.id === model)
    setCustomModel(model.trim().length > 0 && !hasKnownModel)
  }, [activeProvider, activeProviderModels, model])

  useEffect(() => {
    if (!catalog || !activeProvider) return
    writeAssistantLLMPreferences({
      enabled: useLLM,
      provider: activeProvider.id,
      base_url: baseURL.trim() || activeProvider.default_base_url || '',
      model: model.trim(),
      api_key: apiKey.trim(),
      allow_fallback: allowFallback,
    })
  }, [activeProvider, allowFallback, apiKey, baseURL, catalog, model, useLLM])

  const llmReady = !useLLM || (!!activeProvider && model.trim().length > 0 && (!apiKeyRequired || apiKey.trim().length > 0))

  const buildHistory = (): AssistantChatMessage[] => {
    return messages.flatMap((message) => {
      if (message.role === 'user') {
        return [{ role: 'user', content: message.content }]
      }
      if (message.role === 'assistant') {
        return [{
          role: 'assistant',
          content: message.plan.summary,
          sql: message.plan.sql,
          summary: message.plan.summary,
          status: message.plan.status,
          validation_error: message.plan.validation_error,
        }]
      }
      return []
    })
  }

  const handleAsk = async (overridePrompt?: string) => {
    const prompt = (overridePrompt ?? question).trim()
    if (!prompt || loading) return

    let llm: AssistantLLMRequest | undefined
    if (useLLM) {
      if (!activeProvider) {
        setError(catalogError || 'No LLM provider catalog is available right now.')
        return
      }
      llm = {
        enabled: true,
        provider: activeProvider.id,
        base_url: baseURL.trim() || activeProvider.default_base_url || undefined,
        model: model.trim(),
        api_key: apiKey.trim() || undefined,
        allow_fallback: allowFallback,
      }
    }

    const userMessage: AssistantThreadMessage = {
      id: `${Date.now()}-user-${Math.random().toString(36).slice(2, 8)}`,
      role: 'user',
      content: prompt,
    }
    const request: AssistantQueryRequest = {
      question: prompt,
      domains: [domain],
      history: buildHistory(),
      llm,
    }

    setLoading(true)
    setError('')
    setMessages((current) => [...current, userMessage])
    if (!overridePrompt) {
      setQuestion('')
    }
    try {
      const response = await api<AssistantQueryPlan>('/api/assistant/query', 'POST', request)
      setMessages((current) => [...current, {
        id: `${Date.now()}-assistant-${Math.random().toString(36).slice(2, 8)}`,
        role: 'assistant',
        plan: response,
      }])
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err)
      setError(message)
      setMessages((current) => [...current, {
        id: `${Date.now()}-error-${Math.random().toString(36).slice(2, 8)}`,
        role: 'error',
        content: message,
      }])
    } finally {
      setLoading(false)
    }
  }

  const handleKeyDown = (event: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault()
      void handleAsk()
    }
  }

  const isBusy = busy || loading

  const renderAssistantPlan = (plan: AssistantQueryPlan, messageID: string) => {
    const isValid = (plan.status || 'OK') === 'OK'
    return (
      <div key={messageID} className={`ws-assistant-result ${isValid ? '' : 'invalid'}`}>
        <div className="ws-assistant-turn-label">Assistant</div>
        <div className="ws-assistant-meta">
          <span className={`ws-assistant-badge ${plan.confidence || 'medium'}`}>
            {plan.confidence || 'medium'}
          </span>
          {!isValid && <span className="ws-assistant-badge low">needs fix</span>}
          {plan.planner && <span className="ws-assistant-badge neutral">{plan.planner}</span>}
          {plan.provider && <span className="ws-assistant-badge neutral">{plan.provider}</span>}
          {plan.model && <span className="ws-assistant-badge neutral">{plan.model}</span>}
          <span className="ws-assistant-badge neutral">{plan.mode}</span>
          {plan.primary_table && <span className="ws-assistant-badge neutral">{plan.primary_table}</span>}
        </div>

        <p className="ws-assistant-summary">{plan.summary}</p>

        {plan.sql && <pre className="ws-assistant-sql">{plan.sql}</pre>}

        {plan.validation_error && (
          <div className="ws-assistant-list-block warning">
            <div className="ws-assistant-list-title">Validation error</div>
            <div className="ws-assistant-validation-error">{plan.validation_error}</div>
          </div>
        )}

        <div className="ws-assistant-message-actions">
          <button
            className="toolbar-btn"
            onClick={() => plan.sql && onInsertSQL(plan.sql)}
            disabled={!plan.sql || !isValid || isBusy}
            title="Insert SQL into editor"
          >
            <IconCode /> Insert
          </button>
          <button
            className="toolbar-btn accent"
            onClick={() => plan.sql && onRunSQL(plan.sql, plan.primary_table)}
            disabled={!plan.sql || !isValid || isBusy}
            title="Run the generated query"
          >
            <IconPlay /> Run
          </button>
          {useLLM && plan.validation_error && (
            <button
              className="toolbar-btn"
              onClick={() => void handleAsk(AUTO_FIX_PROMPT)}
              disabled={isBusy}
              title="Ask the assistant to repair the rejected SQL"
            >
              <IconCpu /> Fix with this error
            </button>
          )}
        </div>

        {plan.warnings && plan.warnings.length > 0 && (
          <div className="ws-assistant-list-block warning">
            <div className="ws-assistant-list-title">Warnings</div>
            <ul>{plan.warnings.map((w) => <li key={w}>{w}</li>)}</ul>
          </div>
        )}

        {plan.assumptions && plan.assumptions.length > 0 && (
          <div className="ws-assistant-list-block">
            <div className="ws-assistant-list-title">Assumptions</div>
            <ul>{plan.assumptions.map((a) => <li key={a}>{a}</li>)}</ul>
          </div>
        )}

        {plan.matched_columns && plan.matched_columns.length > 0 && (
          <div className="ws-assistant-footnote">
            Matched: {plan.matched_columns.join(', ')}
          </div>
        )}
      </div>
    )
  }

  return (
    <section className="ws-assistant">
      {/* Header */}
      <div className="ws-assistant-header">
        <div className="ws-assistant-title-group">
          <IconCpu />
          <span className="ws-assistant-title">Ask your data</span>
        </div>
        <button className="icon-btn" onClick={onClose} title="Close assistant"><IconX /></button>
      </div>

      <div className="ws-assistant-body">
        {/* Domain badge */}
        <div className="ws-assistant-domain">{domain}</div>

        <div className="ws-assistant-thread">
          {messages.length === 0 && (
            <div className="ws-assistant-empty">
              Ask for a query, then follow up to refine it. Rejected SQL stays visible so you can repair it in context.
            </div>
          )}
          {messages.map((message) => {
            if (message.role === 'user') {
              return (
                <div key={message.id} className="ws-assistant-user-message">
                  <div className="ws-assistant-turn-label">You</div>
                  <div className="ws-assistant-user-bubble">{message.content}</div>
                </div>
              )
            }
            if (message.role === 'assistant') {
              return renderAssistantPlan(message.plan, message.id)
            }
            return (
              <div key={message.id} className="ws-assistant-feedback ws-assistant-feedback-error">
                <IconAlertTriangle />
                <span>{message.content}</span>
              </div>
            )
          })}
        </div>

        {/* Input */}
        <textarea
          className="ws-assistant-input"
          value={question}
          onChange={(event) => setQuestion(event.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask a question or refine the previous SQL…"
          rows={2}
          spellCheck={false}
        />

        {/* Generate */}
        <div className="ws-assistant-actions">
          <button className="toolbar-btn primary ws-assistant-gen-btn" onClick={() => void handleAsk()} disabled={isBusy || !question.trim() || !llmReady}>
            <IconCpu /> {loading ? 'Replying…' : messages.length === 0 ? 'Generate' : 'Send'}
          </button>
        </div>

        {/* Error */}
        {error && (
          <div className="ws-assistant-feedback ws-assistant-feedback-error">
            <IconAlertTriangle />
            <span>{error}</span>
          </div>
        )}

        {latestAssistantPlan?.status === 'INVALID' && useLLM && !loading && (
          <div className="ws-assistant-footnote">
            The last SQL is invalid in ASQL. Ask a follow-up or use “Fix with this error” to repair it.
          </div>
        )}

        {/* LLM Config — collapsible */}
        <div className="ws-assistant-config-section">
          <button className={`ws-assistant-config-toggle ${showConfig ? 'open' : ''}`} onClick={() => setShowConfig(!showConfig)}>
            <IconChevronDown />
            <span>{useLLM ? `LLM: ${activeProvider?.label || provider || 'unconfigured'}${model ? ' / ' + model : ''}` : 'Deterministic planner'}</span>
          </button>

          {showConfig && (
            <div className="ws-assistant-config">
              <label className="ws-assistant-toggle">
                <input type="checkbox" checked={useLLM} onChange={(e) => setUseLLM(e.target.checked)} disabled={isBusy} />
                <span>Use LLM planner</span>
              </label>

              {useLLM && (
                <>
                  <label className="ws-assistant-config-label">
                    <span>Provider</span>
                    <select
                      className="ws-assistant-field"
                      value={activeProvider?.id || provider}
                      onChange={(e) => {
                        const nextProvider = findProvider(catalog, e.target.value)
                        setProvider(e.target.value)
                        setBaseURL(nextProvider?.default_base_url ?? '')
                      }}
                      disabled={isBusy || !catalog || catalog.providers.length === 0}
                    >
                      {(catalog?.providers ?? []).map((option) => (
                        <option key={option.id} value={option.id}>{option.label}</option>
                      ))}
                    </select>
                  </label>

                  <label className="ws-assistant-config-label">
                    <span>Model</span>
                    <>
                      <select
                        className="ws-assistant-field"
                        value={selectedModelValue}
                        onChange={(e) => {
                          const nextValue = e.target.value
                          if (nextValue === CUSTOM_MODEL_SENTINEL) {
                            setCustomModel(true)
                            if (!model.trim()) {
                              setModel(activeProvider?.model_placeholder || '')
                            }
                            return
                          }
                          setCustomModel(false)
                          setModel(nextValue)
                        }}
                        disabled={isBusy || !activeProvider}
                      >
                        {activeProviderModels.length === 0 && (
                          <option value="">No catalog models</option>
                        )}
                        {activeProviderModels.map((option) => (
                          <option key={option.id} value={option.id}>{option.label || option.id}</option>
                        ))}
                        {activeProvider?.supports_custom_model !== false && (
                          <option value={CUSTOM_MODEL_SENTINEL}>Custom model…</option>
                        )}
                      </select>
                      {customModel && activeProvider?.supports_custom_model !== false && (
                        <input
                          className="ws-assistant-field"
                          value={model}
                          onChange={(e) => setModel(e.target.value)}
                          placeholder={activeProvider?.model_placeholder || 'model-id'}
                          spellCheck={false}
                          disabled={isBusy || !activeProvider}
                        />
                      )}
                    </>
                  </label>

                  <label className="ws-assistant-config-label">
                    <span>Base URL</span>
                    <input
                      className="ws-assistant-field"
                      value={baseURL}
                      onChange={(e) => setBaseURL(e.target.value)}
                      placeholder={activeProvider?.default_base_url || ''}
                      spellCheck={false}
                      disabled={isBusy || !activeProvider}
                    />
                  </label>

                  <label className="ws-assistant-config-label">
                    <span>{activeProvider?.api_key_label || 'API key'}</span>
                    <div className="ws-assistant-secret-field">
                      <IconKey />
                      <input
                        className="ws-assistant-field"
                        type="password"
                        value={apiKey}
                        onChange={(e) => setAPIKey(e.target.value)}
                        placeholder={activeProvider?.api_key_placeholder || ''}
                        spellCheck={false}
                        disabled={isBusy || !activeProvider || activeProvider.api_key_mode === 'none'}
                      />
                    </div>
                  </label>

                  <label className="ws-assistant-toggle">
                    <input type="checkbox" checked={allowFallback} onChange={(e) => setAllowFallback(e.target.checked)} disabled={isBusy} />
                    <span>Fallback to deterministic if LLM fails</span>
                  </label>
                </>
              )}

              {useLLM && catalogError && (
                <div className="ws-assistant-footnote">
                  {catalogError}
                </div>
              )}

              {useLLM && !llmReady && (
                <div className="ws-assistant-footnote">
                  Configure a model{apiKeyRequired ? ' and API key' : ''} to enable LLM planning.
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </section>
  )
}
