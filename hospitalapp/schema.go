package main

func schemaPlans() []txPlan {
	return []txPlan{
		{
			name:    "patients schema",
			mode:    "domain",
			domains: []string{"patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE patients.patients (id TEXT PRIMARY KEY, medical_record_no TEXT UNIQUE, full_name TEXT NOT NULL, dob TEXT, sex TEXT, national_id TEXT, consent_status TEXT NOT NULL DEFAULT 'PENDING', risk_level TEXT NOT NULL DEFAULT 'ROUTINE')"},
				{query: "CREATE TABLE patients.patient_contacts (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL REFERENCES patients(id), contact_name TEXT NOT NULL, relation TEXT NOT NULL, phone TEXT NOT NULL)"},
				{query: "CREATE ENTITY patient_entity (ROOT patients, INCLUDES patient_contacts)"},
			},
		},
		{
			name:    "careflow schema",
			mode:    "cross",
			domains: []string{"careflow", "patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE careflow.episodes (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, patient_version INT, pathway_code TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN', origin TEXT NOT NULL, attending_clinician TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE TABLE careflow.encounters (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL REFERENCES episodes(id), patient_id TEXT NOT NULL, patient_version INT, encounter_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'IN_PROGRESS', started_at TEXT NOT NULL, clinician_id TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE ENTITY episode_entity (ROOT episodes, INCLUDES encounters)"},
			},
		},
		{
			name:    "clinical schema",
			mode:    "cross",
			domains: []string{"clinical", "careflow", "patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE clinical.triage_assessments (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, acuity TEXT NOT NULL, complaint TEXT NOT NULL, assessed_by TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'DONE', VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE TABLE clinical.diagnoses (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, code TEXT NOT NULL, description TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ACTIVE', VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
			},
		},
		{
			name:    "orders schema",
			mode:    "cross",
			domains: []string{"orders", "careflow", "patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE orders.lab_orders (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, status TEXT NOT NULL DEFAULT 'REQUESTED', test_code TEXT NOT NULL, ordered_by TEXT NOT NULL, clinical_reason TEXT NOT NULL, ordered_at TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE TABLE orders.medication_orders (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, status TEXT NOT NULL DEFAULT 'ORDERED', medication_code TEXT NOT NULL, dose TEXT NOT NULL, frequency TEXT NOT NULL, ordered_by TEXT NOT NULL, ordered_at TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
			},
		},
		{
			name:       "pharmacy schema",
			mode:       "cross",
			domains:    []string{"pharmacy", "orders", "careflow", "patients"},
			statements: []sqlStatement{{query: "CREATE TABLE pharmacy.dispensations (id TEXT PRIMARY KEY, medication_order_id TEXT NOT NULL, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, dispensed_by TEXT NOT NULL, quantity TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'DISPENSED', VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"}},
		},
		{
			name:    "operations schema",
			mode:    "cross",
			domains: []string{"operations", "careflow", "patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE operations.bed_allocations (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, ward TEXT NOT NULL, bed_code TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'ASSIGNED', assigned_by TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE TABLE operations.surgery_bookings (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, procedure_code TEXT NOT NULL, room_code TEXT NOT NULL, scheduled_at TEXT NOT NULL, surgeon_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PLANNED', VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
			},
		},
		{
			name:       "billing schema",
			mode:       "cross",
			domains:    []string{"billing", "careflow", "patients"},
			statements: []sqlStatement{{query: "CREATE TABLE billing.charges (id TEXT PRIMARY KEY, episode_id TEXT NOT NULL, patient_id TEXT NOT NULL, patient_version INT, charge_type TEXT NOT NULL, description TEXT NOT NULL, amount_cents INT NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'PENDING', VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"}},
		},
		{
			name:    "compliance schema",
			mode:    "cross",
			domains: []string{"compliance", "careflow", "patients"},
			statements: []sqlStatement{
				{query: "CREATE TABLE compliance.signatures (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, patient_version INT, episode_id TEXT, artifact_type TEXT NOT NULL, artifact_id TEXT NOT NULL, signer_id TEXT NOT NULL, meaning TEXT NOT NULL, reason TEXT NOT NULL, signed_at TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
				{query: "CREATE TABLE compliance.audit_events (id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, patient_version INT, episode_id TEXT, event_type TEXT NOT NULL, actor_id TEXT NOT NULL, reason TEXT NOT NULL, artifact_type TEXT NOT NULL, artifact_id TEXT NOT NULL, occurred_at TEXT NOT NULL, payload_json TEXT NOT NULL, VERSIONED FOREIGN KEY (patient_id) REFERENCES patients.patients(id) AS OF patient_version)"},
			},
		},
	}
}
