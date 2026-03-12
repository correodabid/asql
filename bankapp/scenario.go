package main

import (
	"fmt"
	"strings"
)

type txMode string

const (
	txModeDomain txMode = "domain"
	txModeCross  txMode = "cross"
)

const (
	stepIdentitySchema    = "identity schema"
	stepLedgerSchema      = "ledger schema"
	stepPaymentsSchema    = "payments schema"
	stepRiskSchema        = "risk schema"
	stepOnboardAlice      = "onboard alice customer and source account"
	stepOnboardBob        = "onboard bob customer and destination account"
	stepSubmitTransfer    = "submit transfer request"
	stepApproveTransfer   = "approve transfer in risk"
	stepSettleTransfer    = "settle transfer"
	stepUpdateCustomerKYC = "update customer kyc tier"
)

type scenarioStep struct {
	Name       string
	Mode       txMode
	Domains    []string
	Statements []string
}

func (s scenarioStep) BeginSQL() (string, error) {
	if len(s.Domains) == 0 {
		return "", fmt.Errorf("step %q has no domains", s.Name)
	}

	switch s.Mode {
	case txModeDomain:
		if len(s.Domains) != 1 {
			return "", fmt.Errorf("domain step %q must have exactly one domain", s.Name)
		}
		return "BEGIN DOMAIN " + s.Domains[0], nil
	case txModeCross:
		return "BEGIN CROSS DOMAIN " + strings.Join(s.Domains, ", "), nil
	default:
		return "", fmt.Errorf("step %q has unsupported mode %q", s.Name, s.Mode)
	}
}

func schemaSteps() []scenarioStep {
	return []scenarioStep{
		{
			Name:    stepIdentitySchema,
			Mode:    txModeDomain,
			Domains: []string{"identity"},
			Statements: []string{
				"CREATE TABLE identity.customers (id TEXT PRIMARY KEY, legal_name TEXT NOT NULL, segment TEXT NOT NULL, risk_tier TEXT NOT NULL, status TEXT NOT NULL)",
				"CREATE TABLE identity.customer_contacts (id TEXT PRIMARY KEY, customer_id TEXT NOT NULL REFERENCES customers(id), kind TEXT NOT NULL, value TEXT NOT NULL)",
				"CREATE ENTITY customer_entity (ROOT customers, INCLUDES customer_contacts)",
			},
		},
		{
			Name:    stepLedgerSchema,
			Mode:    txModeCross,
			Domains: []string{"ledger", "identity"},
			Statements: []string{
				"CREATE TABLE ledger.accounts (id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, customer_version INT, iban TEXT UNIQUE, currency TEXT NOT NULL, status TEXT NOT NULL, booked_balance_cents INT NOT NULL DEFAULT 0, pending_debit_cents INT NOT NULL DEFAULT 0, available_balance_cents INT NOT NULL DEFAULT 0, VERSIONED FOREIGN KEY (customer_id) REFERENCES identity.customers(id) AS OF customer_version)",
				"CREATE TABLE ledger.account_holds (id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id), reason TEXT NOT NULL, amount_cents INT NOT NULL DEFAULT 0, status TEXT NOT NULL)",
				"CREATE TABLE ledger.ledger_entries (id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id), direction TEXT NOT NULL, amount_cents INT NOT NULL DEFAULT 0, balance_after_cents INT NOT NULL DEFAULT 0, reference_kind TEXT NOT NULL, reference_id TEXT NOT NULL)",
				"CREATE ENTITY account_entity (ROOT accounts, INCLUDES account_holds, ledger_entries)",
			},
		},
		{
			Name:    stepPaymentsSchema,
			Mode:    txModeCross,
			Domains: []string{"payments", "identity", "ledger"},
			Statements: []string{
				"CREATE TABLE payments.transfer_requests (id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, customer_version INT, source_account_id TEXT NOT NULL, source_account_version INT, destination_account_id TEXT NOT NULL, destination_account_version INT, amount_cents INT NOT NULL DEFAULT 0, status TEXT NOT NULL, created_reason TEXT NOT NULL, VERSIONED FOREIGN KEY (customer_id) REFERENCES identity.customers(id) AS OF customer_version, VERSIONED FOREIGN KEY (source_account_id) REFERENCES ledger.accounts(id) AS OF source_account_version, VERSIONED FOREIGN KEY (destination_account_id) REFERENCES ledger.accounts(id) AS OF destination_account_version)",
				"CREATE TABLE payments.transfer_events (id TEXT PRIMARY KEY, transfer_id TEXT NOT NULL REFERENCES transfer_requests(id), event_type TEXT NOT NULL, note TEXT NOT NULL)",
				"CREATE ENTITY transfer_entity (ROOT transfer_requests, INCLUDES transfer_events)",
			},
		},
		{
			Name:    stepRiskSchema,
			Mode:    txModeCross,
			Domains: []string{"risk", "payments"},
			Statements: []string{
				"CREATE TABLE risk.risk_reviews (id TEXT PRIMARY KEY, transfer_id TEXT NOT NULL, transfer_version INT, decision TEXT NOT NULL, analyst_note TEXT NOT NULL, VERSIONED FOREIGN KEY (transfer_id) REFERENCES payments.transfer_requests(id) AS OF transfer_version)",
				"CREATE ENTITY risk_review_entity (ROOT risk_reviews)",
			},
		},
	}
}

func workflowSteps() []scenarioStep {
	return []scenarioStep{
		{
			Name:    stepOnboardAlice,
			Mode:    txModeCross,
			Domains: []string{"identity", "ledger"},
			Statements: []string{
				"INSERT INTO identity.customers (id, legal_name, segment, risk_tier, status) VALUES ('cust-001', 'Alice Example', 'retail', 'standard', 'active')",
				"INSERT INTO identity.customer_contacts (id, customer_id, kind, value) VALUES ('contact-001', 'cust-001', 'email', 'alice@example.bank')",
				"INSERT INTO ledger.accounts (id, customer_id, iban, currency, status, booked_balance_cents, pending_debit_cents, available_balance_cents) VALUES ('acct-001', 'cust-001', 'ES1000000000000000000001', 'EUR', 'ACTIVE', 125000, 0, 125000)",
			},
		},
		{
			Name:    stepOnboardBob,
			Mode:    txModeCross,
			Domains: []string{"identity", "ledger"},
			Statements: []string{
				"INSERT INTO identity.customers (id, legal_name, segment, risk_tier, status) VALUES ('cust-002', 'Bob Example', 'retail', 'standard', 'active')",
				"INSERT INTO identity.customer_contacts (id, customer_id, kind, value) VALUES ('contact-002', 'cust-002', 'email', 'bob@example.bank')",
				"INSERT INTO ledger.accounts (id, customer_id, iban, currency, status, booked_balance_cents, pending_debit_cents, available_balance_cents) VALUES ('acct-002', 'cust-002', 'ES1000000000000000000002', 'EUR', 'ACTIVE', 50000, 0, 50000)",
			},
		},
		{
			Name:    stepSubmitTransfer,
			Mode:    txModeCross,
			Domains: []string{"payments", "identity", "ledger"},
			Statements: []string{
				"INSERT INTO payments.transfer_requests (id, customer_id, source_account_id, destination_account_id, amount_cents, status, created_reason) VALUES ('tr-001', 'cust-001', 'acct-001', 'acct-002', 2500, 'PENDING_RISK', 'scheduled rent payment')",
				"INSERT INTO payments.transfer_events (id, transfer_id, event_type, note) VALUES ('te-001', 'tr-001', 'SUBMITTED', 'transfer submitted to risk queue')",
				"UPDATE ledger.accounts SET pending_debit_cents = 2500, available_balance_cents = 122500 WHERE id = 'acct-001'",
				"INSERT INTO ledger.ledger_entries (id, account_id, direction, amount_cents, balance_after_cents, reference_kind, reference_id) VALUES ('le-001', 'acct-001', 'PENDING_DEBIT', 2500, 122500, 'transfer_request', 'tr-001')",
			},
		},
		{
			Name:    stepApproveTransfer,
			Mode:    txModeCross,
			Domains: []string{"risk", "payments"},
			Statements: []string{
				"INSERT INTO risk.risk_reviews (id, transfer_id, decision, analyst_note) VALUES ('rr-001', 'tr-001', 'APPROVE', 'ruleset v1 accepted deterministic transfer profile')",
				"UPDATE payments.transfer_requests SET status = 'RISK_APPROVED' WHERE id = 'tr-001'",
				"INSERT INTO payments.transfer_events (id, transfer_id, event_type, note) VALUES ('te-002', 'tr-001', 'RISK_APPROVED', 'risk team approved transfer')",
			},
		},
		{
			Name:    stepSettleTransfer,
			Mode:    txModeCross,
			Domains: []string{"payments", "ledger"},
			Statements: []string{
				"UPDATE payments.transfer_requests SET status = 'SETTLED' WHERE id = 'tr-001'",
				"INSERT INTO payments.transfer_events (id, transfer_id, event_type, note) VALUES ('te-003', 'tr-001', 'SETTLED', 'ledger balances moved to booked state')",
				"UPDATE ledger.accounts SET booked_balance_cents = 122500, pending_debit_cents = 0, available_balance_cents = 122500 WHERE id = 'acct-001'",
				"UPDATE ledger.accounts SET booked_balance_cents = 52500, available_balance_cents = 52500 WHERE id = 'acct-002'",
				"INSERT INTO ledger.ledger_entries (id, account_id, direction, amount_cents, balance_after_cents, reference_kind, reference_id) VALUES ('le-002', 'acct-001', 'BOOKED_DEBIT', 2500, 122500, 'transfer_request', 'tr-001')",
				"INSERT INTO ledger.ledger_entries (id, account_id, direction, amount_cents, balance_after_cents, reference_kind, reference_id) VALUES ('le-003', 'acct-002', 'BOOKED_CREDIT', 2500, 52500, 'transfer_request', 'tr-001')",
			},
		},
		{
			Name:    stepUpdateCustomerKYC,
			Mode:    txModeDomain,
			Domains: []string{"identity"},
			Statements: []string{
				"UPDATE identity.customers SET risk_tier = 'enhanced' WHERE id = 'cust-001'",
			},
		},
	}
}

func allSteps() []scenarioStep {
	steps := make([]scenarioStep, 0, len(schemaSteps())+len(workflowSteps()))
	steps = append(steps, schemaSteps()...)
	steps = append(steps, workflowSteps()...)
	return steps
}
