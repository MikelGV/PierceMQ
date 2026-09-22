package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/MikelGV/PierceMQ/pkg/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// doPost is a minimal authed POST helper for endpoints the Go client does not
// wrap (e.g. key revocation).
func doPost(c *client.Client, path string, body any, out any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.BaseURL+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errBody map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errBody)
		return fmt.Errorf("POST %s -> %d %v", path, resp.StatusCode, errBody)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func TestRegisterAndLogin(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := &client.Client{BaseURL: fx.server.URL}

	t.Run("register ok then duplicate is 409", func(t *testing.T) {
		out, err := c.Register(ctx, "Ada", "ada@example.com", "password123")
		require.NoError(t, err)
		require.NotEmpty(t, out["user_id"])

		_, err = c.Register(ctx, "Ada", "ada@example.com", "password123")
		require.Error(t, err)
		require.Contains(t, err.Error(), "409")
	})

	t.Run("register rejects weak input", func(t *testing.T) {
		_, err := c.Register(ctx, "", "bad", "short")
		require.Error(t, err)
		require.Contains(t, err.Error(), "400")
	})

	t.Run("login ok, wrong password is 401", func(t *testing.T) {
		_, err := c.Register(ctx, "Grace", "grace@example.com", "password123")
		require.NoError(t, err)

		token, err := c.Login(ctx, "grace@example.com", "password123")
		require.NoError(t, err)
		require.NotEmpty(t, token)

		bad := &client.Client{BaseURL: fx.server.URL}
		_, err = bad.Login(ctx, "grace@example.com", "wrong-password")
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")

		_, err = bad.Login(ctx, "nobody@example.com", "password123")
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")
	})

	t.Run("jwt grants access, missing and bogus tokens do not", func(t *testing.T) {
		_, err := c.Register(ctx, "Linus", "linus@example.com", "password123")
		require.NoError(t, err)
		_, err = c.Login(ctx, "linus@example.com", "password123")
		require.NoError(t, err)

		// Authenticated but unknown job -> 404 proves the JWT was accepted.
		_, err = c.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")

		anon := &client.Client{BaseURL: fx.server.URL}
		_, err = anon.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")

		bogus := &client.Client{BaseURL: fx.server.URL, Token: "bogus.token.here"}
		_, err = bogus.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")
	})
}

func TestAPIKeys(t *testing.T) {
	fx := setupAPI(t)
	ctx := context.Background()
	c := &client.Client{BaseURL: fx.server.URL}

	_, err := c.Register(ctx, "Key User", "keys@example.com", "password123")
	require.NoError(t, err)
	_, err = c.Login(ctx, "keys@example.com", "password123")
	require.NoError(t, err)

	t.Run("create key, use it, revoke it", func(t *testing.T) {
		created, err := c.CreateAPIKey(ctx, "worker-1")
		require.NoError(t, err)
		require.NotEmpty(t, created["key_id"])
		require.True(t, strings.HasPrefix(created["key"], "pmq_"), "plaintext must be shown once")

		svc := &client.Client{BaseURL: fx.server.URL, Token: created["key"]}
		_, err = svc.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "404", "service key must authenticate")

		// Revoke via the JWT session.
		var out map[string]string
		err = doPost(c, "/v1/keys/revoke", map[string]string{"key_id": created["key_id"]}, &out)
		require.NoError(t, err)
		require.Equal(t, "revoked", out["status"])

		_, err = svc.GetJob(ctx, uuid.New().String())
		require.Error(t, err)
		require.Contains(t, err.Error(), "401", "revoked key must stop working")
	})

	t.Run("revoke unknown key is 404", func(t *testing.T) {
		var out map[string]string
		err := doPost(c, "/v1/keys/revoke", map[string]string{"key_id": uuid.New().String()}, &out)
		require.Error(t, err)
		require.Contains(t, err.Error(), "404")
	})
}
