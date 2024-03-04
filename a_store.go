package main

import "sync"

type StorageEngine interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Snapshot() (map[string]string, error)
	Restore(o map[string]string) error
}

type MemStorageEngine struct {
	mu   sync.Mutex
	data map[string]string
}

func NewMemStorageEngine() StorageEngine {
	return &MemStorageEngine{
		data: make(map[string]string),
	}
}

func (s *MemStorageEngine) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], nil
}

func (s *MemStorageEngine) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *MemStorageEngine) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}

func (s *MemStorageEngine) Snapshot() (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := make(map[string]string)
	for k, v := range s.data {
		o[k] = v
	}
	return o, nil
}

func (s *MemStorageEngine) Restore(o map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = o
	return nil
}
