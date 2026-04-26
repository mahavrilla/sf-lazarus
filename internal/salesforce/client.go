package salesforce

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const defaultAPIVersion = "v59.0"

// Record is one row returned by the Salesforce Bulk API or REST API.
// All values are strings — callers coerce to the target type.
type Record map[string]string

// Config holds credentials and endpoint configuration for a Salesforce org.
type Config struct {
	// LoginURL is the OAuth2 token endpoint host. Defaults to
	// "https://login.salesforce.com".
	LoginURL string

	// ClientID and ClientSecret are the Connected App credentials used for
	// the OAuth2 client credentials flow.
	ClientID     string
	ClientSecret string

	// APIVersion selects the REST API version. Defaults to "v59.0".
	APIVersion string
}

// Client is an authenticated Salesforce REST client.
type Client struct {
	instanceURL string
	accessToken string
	apiVersion  string
	http        *http.Client
}

// NewClient authenticates via the OAuth2 client credentials flow and returns
// a ready-to-use Client.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	version := cfg.APIVersion
	if version == "" {
		version = defaultAPIVersion
	}

	token, instanceURL, err := clientCredentialsAuth(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("salesforce auth: %w", err)
	}

	return &Client{
		instanceURL: strings.TrimRight(instanceURL, "/"),
		accessToken: token,
		apiVersion:  version,
		http:        &http.Client{},
	}, nil
}

// clientCredentialsAuth performs the OAuth2 client credentials flow.
func clientCredentialsAuth(ctx context.Context, cfg Config) (token, instanceURL string, err error) {
	loginURL := cfg.LoginURL
	if loginURL == "" {
		loginURL = "https://login.salesforce.com"
	}

	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {cfg.ClientID},
		"client_secret": {cfg.ClientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		loginURL+"/services/oauth2/token",
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	var result struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", "", fmt.Errorf("decode auth response: %w", err)
	}
	if result.Error != "" {
		return "", "", fmt.Errorf("%s: %s", result.Error, result.Description)
	}
	return result.AccessToken, result.InstanceURL, nil
}

// endpoint returns the full URL for the given versioned path, e.g.
// "/jobs/query" → "https://org.salesforce.com/services/data/v59.0/jobs/query".
func (c *Client) endpoint(path string) string {
	return fmt.Sprintf("%s/services/data/%s%s", c.instanceURL, c.apiVersion, path)
}

// do attaches the Bearer token and executes the request.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	return c.http.Do(req)
}

// postJSON marshals body to JSON, POSTs it, and decodes the response into dst.
// Pass dst=nil to discard the response body.
func (c *Client) postJSON(ctx context.Context, path string, body, dst any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint(path), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: HTTP %d: %s", path, resp.StatusCode, raw)
	}
	if dst == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// getJSON GETs the path and decodes the JSON response into dst.
func (c *Client) getJSON(ctx context.Context, path string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint(path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, raw)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// patchJSON marshals body to JSON, PATCHes it, and decodes the response into dst.
// Pass dst=nil to discard the response body.
func (c *Client) patchJSON(ctx context.Context, path string, body, dst any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch,
		c.endpoint(path), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH %s: HTTP %d: %s", path, resp.StatusCode, raw)
	}
	if dst == nil {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// deleteResource sends a DELETE to the given path and discards the response.
func (c *Client) deleteResource(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.endpoint(path), nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s: HTTP %d", path, resp.StatusCode)
	}
	return nil
}
