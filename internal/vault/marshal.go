package vault

import "encoding/json"

func marshalSecrets(m map[string]string) ([]byte, error) {
	return json.Marshal(m)
}

func unmarshalSecrets(b []byte) (map[string]string, error) {
	var m map[string]string
	err := json.Unmarshal(b, &m)
	return m, err
}
