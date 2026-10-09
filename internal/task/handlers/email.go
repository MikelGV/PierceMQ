package handlers

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// EmailJob is a validated send_email payload.
type EmailJob struct {
	To      string
	From    string
	Subject string
	Body    string
	JobID   string // for the Message-Id header (at-least-once dedup hint)
}

// EmailConfig configures SMTP delivery. DryRun (default when SMTP_HOST is
// empty or EMAIL_DRY_RUN=1) validates and logs without touching the network,
// preserving the old stub behaviour for local dev and unit tests.
type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string // default From when payload omits it ("" = required)
	Timeout  time.Duration
	DryRun   bool
}

// EmailConfigFromEnv builds an EmailConfig from SMTP_HOST/PORT/USER/PASS,
// SMTP_FROM, SMTP_TIMEOUT_SEC and EMAIL_DRY_RUN.
func EmailConfigFromEnv(getenv func(string) string) EmailConfig {
	if getenv == nil {
		getenv = os.Getenv
	}
	port := 587
	if v := strings.TrimSpace(getenv("SMTP_PORT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n < 65536 {
			port = n
		}
	}
	timeout := 15 * time.Second
	if v := strings.TrimSpace(getenv("SMTP_TIMEOUT_SEC")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}
	host := strings.TrimSpace(getenv("SMTP_HOST"))
	dry := host == ""
	if v := strings.TrimSpace(getenv("EMAIL_DRY_RUN")); v == "1" || strings.EqualFold(v, "true") {
		dry = true
	}
	return EmailConfig{
		Host:     host,
		Port:     port,
		Username: strings.TrimSpace(getenv("SMTP_USER")),
		Password: getenv("SMTP_PASS"),
		From:     strings.TrimSpace(getenv("SMTP_FROM")),
		Timeout:  timeout,
		DryRun:   dry,
	}
}

// ParseEmailPayload validates a decoded email payload. Accepts the legacy
// stub fields (to/from) plus optional subject/body.
func ParseEmailPayload(payload map[string]any) (EmailJob, error) {
	to, ok := strField(payload, "to")
	if !ok {
		return EmailJob{}, PermanentMsg("email job missing required field: to")
	}
	from, ok := strField(payload, "from")
	if !ok {
		return EmailJob{}, PermanentMsg("email job missing required field: from")
	}
	if _, err := mail.ParseAddress(to); err != nil {
		return EmailJob{}, PermanentMsg("email job invalid to address %q: %v", to, err)
	}
	if _, err := mail.ParseAddress(from); err != nil {
		return EmailJob{}, PermanentMsg("email job invalid from address %q: %v", from, err)
	}
	subject, _ := strField(payload, "subject")
	body, _ := strField(payload, "body", "text", "content")
	jobID, _ := strField(payload, "job_id", "jobId")
	return EmailJob{To: to, From: from, Subject: subject, Body: body, JobID: jobID}, nil
}

// Sender delivers an EmailJob. Returns the provider message-id (or the
// dry-run id) for logging.
type Sender interface {
	Send(ctx context.Context, job EmailJob) (string, error)
}

// SMTPSender is the production Sender.
type SMTPSender struct {
	Cfg EmailConfig
}

// Send validates, then delivers via SMTP with STARTTLS when offered.
// Auth failures and address errors are Permanent; dial/timeout/5xx are
// transient (retry via FailOrRetry backoff). The Message-Id header carries
// the PierceMQ job id so duplicate deliveries (at-least-once) can be
// recognised downstream. SMTP itself has no dedup — retries may duplicate.
func (s SMTPSender) Send(ctx context.Context, job EmailJob) (string, error) {
	if _, err := mail.ParseAddress(job.To); err != nil {
		return "", PermanentMsg("invalid to address: %v", err)
	}
	if _, err := mail.ParseAddress(job.From); err != nil {
		return "", PermanentMsg("invalid from address: %v", err)
	}
	msgID := job.JobID
	if msgID == "" {
		msgID = uuid.NewString()
	}
	if s.Cfg.DryRun || s.Cfg.Host == "" {
		return fmt.Sprintf("dry-run:%s to=%s", msgID, job.To), nil
	}

	msg := buildMessage(job, msgID)

	addr := net.JoinHostPort(s.Cfg.Host, strconv.Itoa(s.Cfg.Port))
	dialer := &net.Dialer{Timeout: s.Cfg.Timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", fmt.Errorf("smtp dial %s: %w", addr, err)
	}
	c, err := smtp.NewClient(conn, s.Cfg.Host)
	if err != nil {
		return "", fmt.Errorf("smtp client: %w", err)
	}
	defer c.Close()

	if ok, _ := c.Extension("STARTTLS"); ok {
		tlsCfg := &tls.Config{ServerName: s.Cfg.Host} //nolint:gosec // opportunistic STARTTLS like most MTAs
		if err := c.StartTLS(tlsCfg); err != nil {
			return "", fmt.Errorf("smtp starttls: %w", err)
		}
	}
	if s.Cfg.Username != "" {
		auth := smtp.PlainAuth("", s.Cfg.Username, s.Cfg.Password, s.Cfg.Host)
		if err := c.Auth(auth); err != nil {
			return "", PermanentMsg("smtp auth failed: %v", err)
		}
	}
	if err := c.Mail(job.From); err != nil {
		return "", PermanentMsg("smtp MAIL FROM rejected: %v", err)
	}
	if err := c.Rcpt(job.To); err != nil {
		return "", PermanentMsg("smtp RCPT TO rejected: %v", err)
	}
	wc, err := c.Data()
	if err != nil {
		return "", fmt.Errorf("smtp DATA: %w", err)
	}
	if _, err := wc.Write(msg); err != nil {
		_ = wc.Close()
		return "", fmt.Errorf("smtp write: %w", err)
	}
	if err := wc.Close(); err != nil {
		return "", fmt.Errorf("smtp data close: %w", err)
	}
	if err := c.Quit(); err != nil {
		return "", fmt.Errorf("smtp quit: %w", err)
	}
	return msgID, nil
}

func buildMessage(job EmailJob, msgID string) []byte {
	var b strings.Builder
	//b.WriteString("From: " + job.From + "\r\n")
	b.WriteString("From:")
	b.WriteString(job.From)
	b.WriteString("\r\n")
	//b.WriteString("To: " + job.To + "\r\n")
	b.WriteString("To:")
	b.WriteString(job.From)
	b.WriteString("\r\n")
	sub := job.Subject
	if sub == "" {
		sub = "(no subject)"
	}
	//b.WriteString("Subject: " + sub + "\r\n")
	b.WriteString("Subject:")
	b.WriteString(sub)
	b.WriteString("\r\n")
	//b.WriteString("Message-Id: <" + msgID + "@piercemq>\r\n")
	b.WriteString("Message-Id: <")
	b.WriteString(msgID)
	b.WriteString("\r\n")
	//b.WriteString("Date: " + time.Now().UTC().Format(time.RFC1123Z) + "\r\n")
	b.WriteString("Date: ")
	b.WriteString(time.Now().UTC().Format(time.RFC1123Z))
	b.WriteString("\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	//b.WriteString("\r\n" + job.Body + "\r\n")
	b.WriteString("\r\n")
	b.WriteString(job.Body)
	b.WriteString("\r\n")
	return []byte(b.String())
}
