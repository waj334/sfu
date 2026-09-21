package sfu

import "sync"

type clientTrackList struct {
	mu       sync.RWMutex
	tracks   []iClientTrack
	onAdd    func(iClientTrack)
	onRemove func(iClientTrack)
}

func (l *clientTrackList) Add(track iClientTrack) {
	l.mu.Lock()

	// TODO: change to non go routine
	track.OnEnded(func() {
		l.remove(track)
	})

	l.tracks = append(l.tracks, track)
	onAdd := l.onAdd
	l.mu.Unlock()
	if onAdd != nil {
		onAdd(track)
	}
}

func (l *clientTrackList) remove(target iClientTrack) {
	l.mu.Lock()

	for i, track := range l.tracks {
		if track == target {
			copy(l.tracks[i:], l.tracks[i+1:])
			l.tracks[len(l.tracks)-1] = nil
			l.tracks = l.tracks[:len(l.tracks)-1]
			if len(l.tracks) == 0 {
				l.tracks = nil
			}
			break
		}
	}

	onRemove := l.onRemove
	l.mu.Unlock()
	if onRemove != nil {
		onRemove(target)
	}
}

func (l *clientTrackList) OnChanged(onAdd, onRemove func(iClientTrack)) {
	l.mu.Lock()
	l.onAdd = onAdd
	l.onRemove = onRemove
	existing := append([]iClientTrack(nil), l.tracks...)
	l.mu.Unlock()

	for _, track := range existing {
		onAdd(track)
	}
}

func (l *clientTrackList) Get(id string) iClientTrack {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, t := range l.tracks {
		if t.ID() == id {
			return t
		}
	}

	return nil
}

func (l *clientTrackList) Length() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.tracks)
}

func (l *clientTrackList) GetTracks() []iClientTrack {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.tracks) == 0 {
		return nil
	}
	clientTracks := make([]iClientTrack, len(l.tracks))
	copy(clientTracks, l.tracks)
	return clientTracks
}

func newClientTrackList() *clientTrackList {
	return &clientTrackList{
		mu:     sync.RWMutex{},
		tracks: make([]iClientTrack, 0),
	}
}
