package lib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

type StorageClient struct {
	Host string
}

type File struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Size int64  `json:"size"`
}

func GetClient(host string) *StorageClient {
	client := StorageClient{}
	client.Host = host

	return &client
}

func (c *StorageClient) List(prefix string) []File {

	client := &http.Client{}

	getURL := fmt.Sprintf(c.Host+"/v1/list?q=%v", prefix)
	log.Println("Storage - Request " + getURL)

	req, err := http.NewRequest("GET", getURL, nil)

	resp, err := client.Do(req)
	if err != nil {
		log.Println("Storage - Failed to do request:", err)
		return []File{}
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Storage - Failed to read response: ", err)
		return []File{}
	}

	ret := []File{}

	json.Unmarshal(respBody, &ret)

	return ret

}

func (c *StorageClient) Get(path string) []byte {

	client := &http.Client{}

	getURL := fmt.Sprintf(c.Host+"/v1/get?p=%v", path)
	log.Println("Storage - Request " + getURL)

	req, err := http.NewRequest("GET", getURL, nil)

	resp, err := client.Do(req)
	if err != nil {
		log.Println("Storage - Failed to do request:", err)
		return []byte{}
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println("Storage - Failed to read response: ", err)
		return []byte{}
	}

	return respBody

}

func (c *StorageClient) Save(path string, content []byte) error {

	client := &http.Client{}

	postURL := c.Host+"/v1/save"
	log.Println("Storage - Request " + postURL)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("c", filepath.Base(path))
	if err != nil {
		return err
	}

	if _, err = part.Write(content); err != nil {
		return err
	}

	if err = writer.WriteField("p", path); err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", postURL, body)
	req.Header.Add("Content-Type", writer.FormDataContentType())

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Storage - Failed to do request: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Storage - Failed to read response: %v", err)
	}
	log.Println("Storage - Save", resp.Status, respBody)

	return nil

}

func (c *StorageClient) Move(pathSource string, pathTarget string) error {

	client := &http.Client{}

	postURL := c.Host+"/v1/move"
	log.Println("Storage - Request "+postURL, pathSource, pathTarget)

	form := url.Values{}
	form.Add("s", pathSource)
	form.Add("t", pathTarget)

	req, err := http.NewRequest("POST", postURL, strings.NewReader(form.Encode()))
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("Storage - Failed to do request: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("Storage - Failed to read response: %v", err)
	}
	log.Println("Storage - Move", resp.Status, respBody)

	return nil

}
