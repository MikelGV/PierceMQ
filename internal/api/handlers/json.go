package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
)

// errBodyTooLarge signals a request envelope over the decode cap. Callers
// map it to 413; all other decode errors are 400.
var errBodyTooLarge = errors.New("request body too large")

// MaxSmallBodyBytes caps auth/key JSON envelopes (register/login/keys).
// These shapes are tiny; 64KB leaves ample headroom while bounding abuse.
const MaxSmallBodyBytes = 64 * 1024

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON bounds the envelope before decoding: an explicit ContentLength
// over the cap fails fast with errBodyTooLarge, and MaxBytesReader enforces
// the cap on chunked bodies (Decode then surfaces *http.MaxBytesError,
// normalized to errBodyTooLarge). Unknown fields are rejected.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	return decodeBoundedJSON(w, r, v, MaxSmallBodyBytes)
}

func decodeBoundedJSON(w http.ResponseWriter, r *http.Request, v any, maxBytes int64) error {
	if r.ContentLength > maxBytes {
		return errBodyTooLarge
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return errBodyTooLarge
		}
		return err
	}
	return nil
}

// writeDecodeError maps decode failures to 413 (envelope over cap) or 400.
func writeDecodeError(w http.ResponseWriter, err error) {
	if errors.Is(err, errBodyTooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
}
