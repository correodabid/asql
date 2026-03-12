package asqldb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/hospital-miks/backend/internal/domain/model"
)

const domainMessaging = "messaging"

// ── MessageRepo ─────────────────────────────────────────────────────────────

type MessageRepo struct{ client *Client }

func NewMessageRepo(c *Client) *MessageRepo { return &MessageRepo{client: c} }

func (r *MessageRepo) Create(ctx context.Context, m *model.Message) error {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return err
	}
	parentID := "NULL"
	if m.ParentID != nil {
		parentID = sqlStr(m.ParentID.String())
	}
	sql := fmt.Sprintf(
		`INSERT INTO messages (id, sender_id, receiver_id, subject, body, priority, read, read_at, parent_id, created_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(m.ID.String()), sqlStr(m.SenderID.String()),
		sqlStr(m.ReceiverID.String()), sqlStr(m.Subject), sqlStr(m.Body),
		sqlStr(string(m.Priority)), boolToSQL(m.Read),
		nullableTS(m.ReadAt), parentID,
		sqlStr(ts(m.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MessageRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.Message, error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM messages WHERE id = %s", msgCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("message not found: %s", id)
	}
	return scanMessage(rows)
}

func (r *MessageRepo) MarkRead(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("UPDATE messages SET read = true, read_at = %s WHERE id = %s",
		sqlStr(ts(timeNow())), sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MessageRepo) Delete(ctx context.Context, id uuid.UUID) error {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("DELETE FROM messages WHERE id = %s", sqlStr(id.String()))
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *MessageRepo) ListInbox(ctx context.Context, userID uuid.UUID, f model.ListFilter) (*model.ListResult[model.Message], error) {
	where := fmt.Sprintf("WHERE receiver_id = %s", sqlStr(userID.String()))
	return r.listWhere(ctx, where, f)
}

func (r *MessageRepo) ListSent(ctx context.Context, userID uuid.UUID, f model.ListFilter) (*model.ListResult[model.Message], error) {
	where := fmt.Sprintf("WHERE sender_id = %s", sqlStr(userID.String()))
	return r.listWhere(ctx, where, f)
}

func (r *MessageRepo) CountUnread(ctx context.Context, userID uuid.UUID) (int, error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT COUNT(*) FROM messages WHERE receiver_id = %s AND read = false", sqlStr(userID.String()))
	return countQuery(ctx, tx, sql), nil
}

func (r *MessageRepo) ListThread(ctx context.Context, parentID uuid.UUID) ([]model.Message, error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM messages WHERE parent_id = %s ORDER BY created_at ASC", msgCols, sqlStr(parentID.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return items, rows.Err()
}

func (r *MessageRepo) listWhere(ctx context.Context, where string, f model.ListFilter) (*model.ListResult[model.Message], error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM messages "+where)
	sql := "SELECT " + msgCols + " FROM messages " + where + orderClause(f, "created_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *m)
	}
	return buildListResult(items, total, f), rows.Err()
}

const msgCols = "id, sender_id, receiver_id, subject, body, priority, read, read_at, parent_id, created_at"

func scanMessage(rows pgx.Rows) (*model.Message, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	m := &model.Message{}
	if len(vals) >= 10 {
		m.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		m.SenderID = parseUUID(fmt.Sprintf("%v", vals[1]))
		m.ReceiverID = parseUUID(fmt.Sprintf("%v", vals[2]))
		m.Subject = fmt.Sprintf("%v", vals[3])
		m.Body = fmt.Sprintf("%v", vals[4])
		m.Priority = model.MessagePriority(fmt.Sprintf("%v", vals[5]))
		m.Read = fmt.Sprintf("%v", vals[6]) == "true"
		if v := fmt.Sprintf("%v", vals[7]); v != "" && v != "<nil>" {
			t := parseTS(v)
			m.ReadAt = &t
		}
		if v := fmt.Sprintf("%v", vals[8]); v != "" && v != "<nil>" {
			id := parseUUID(v)
			m.ParentID = &id
		}
		m.CreatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
	}
	return m, nil
}

// ── PatientCommunicationRepo ────────────────────────────────────────────────

type PatientCommunicationRepo struct{ client *Client }

func NewPatientCommunicationRepo(c *Client) *PatientCommunicationRepo {
	return &PatientCommunicationRepo{client: c}
}

func (r *PatientCommunicationRepo) Create(ctx context.Context, c *model.PatientCommunication) error {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(
		`INSERT INTO patient_communications (id, patient_id, staff_id, channel, subject, content, status, sent_at, delivered_at, created_at)
		 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		sqlStr(c.ID.String()), sqlStr(c.PatientID.String()),
		sqlStr(c.StaffID.String()), sqlStr(c.Channel),
		sqlStr(c.Subject), sqlStr(c.Content), sqlStr(c.Status),
		sqlStr(ts(c.SentAt)), nullableTS(c.DeliveredAt),
		sqlStr(ts(c.CreatedAt)),
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

func (r *PatientCommunicationRepo) GetByID(ctx context.Context, id uuid.UUID) (*model.PatientCommunication, error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	sql := fmt.Sprintf("SELECT %s FROM patient_communications WHERE id = %s", commCols, sqlStr(id.String()))
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, fmt.Errorf("communication not found: %s", id)
	}
	return scanComm(rows)
}

func (r *PatientCommunicationRepo) ListByPatient(ctx context.Context, pid uuid.UUID, f model.ListFilter) (*model.ListResult[model.PatientCommunication], error) {
	tx, err := r.client.BeginDomain(ctx, domainMessaging)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	where := fmt.Sprintf("WHERE patient_id = %s", sqlStr(pid.String()))
	total := countQuery(ctx, tx, "SELECT COUNT(*) FROM patient_communications "+where)
	sql := "SELECT " + commCols + " FROM patient_communications " + where + orderClause(f, "sent_at") + paginationClause(f)
	rows, err := tx.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []model.PatientCommunication
	for rows.Next() {
		c, err := scanComm(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *c)
	}
	return buildListResult(items, total, f), rows.Err()
}

const commCols = "id, patient_id, staff_id, channel, subject, content, status, sent_at, delivered_at, created_at"

func scanComm(rows pgx.Rows) (*model.PatientCommunication, error) {
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	c := &model.PatientCommunication{}
	if len(vals) >= 10 {
		c.ID = parseUUID(fmt.Sprintf("%v", vals[0]))
		c.PatientID = parseUUID(fmt.Sprintf("%v", vals[1]))
		c.StaffID = parseUUID(fmt.Sprintf("%v", vals[2]))
		c.Channel = fmt.Sprintf("%v", vals[3])
		c.Subject = fmt.Sprintf("%v", vals[4])
		c.Content = fmt.Sprintf("%v", vals[5])
		c.Status = fmt.Sprintf("%v", vals[6])
		c.SentAt = parseTS(fmt.Sprintf("%v", vals[7]))
		if v := fmt.Sprintf("%v", vals[8]); v != "" && v != "<nil>" {
			t := parseTS(v)
			c.DeliveredAt = &t
		}
		c.CreatedAt = parseTS(fmt.Sprintf("%v", vals[9]))
	}
	return c, nil
}
