package ga

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

func SendEvent(row internal.Event, event string, logger *zap.SugaredLogger) error {
	postBody := map[string]interface{}{
		"client_id": row.RemoteIP,
		"events": []map[string]interface{}{
			{
				"name":   event,
				"params": row,
			},
		},
	}

	return doRequest(postBody, logger)
}
func doRequest(body map[string]interface{}, logger *zap.SugaredLogger) error {
	jsonValue, err := json.Marshal(body)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", "https://www.google-analytics.com/mp/collect", bytes.NewBuffer(jsonValue))
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("measurement_id", "G-S6NT05VXK9")
	q.Add("api_secret", "7FFhe65HQK-bXvujpQMquQ")
	req.URL.RawQuery = q.Encode()

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode/100 != 2 {
		dump, err := httputil.DumpResponse(response, true)
		if err != nil {
			return err
		}
		logger.Errorf("error while posting to GA: %v", string(dump))
		return fmt.Errorf("google analytics returned non-2xx status code: %v", response.Status)
	}

	return nil
}
