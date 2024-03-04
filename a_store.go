package main

import "sync"

type Store struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data[key], nil
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}
