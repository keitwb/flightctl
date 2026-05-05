// Copyright (c) Flight Control Authors. Licensed under Apache-2.0.

package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/flightctl/flightctl/internal/flterrors"
	"github.com/flightctl/flightctl/internal/store/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type SyncState interface {
	InitialMigration(ctx context.Context) error
	Get(ctx context.Context, orgID uuid.UUID, resourceKey string) (*model.SyncState, error)
	Set(ctx context.Context, orgID uuid.UUID, state *model.SyncState) error
	List(ctx context.Context, orgID uuid.UUID) ([]model.SyncState, error)
}

type SyncStateStore struct {
	dbHandler *gorm.DB
	log       logrus.FieldLogger
}

var _ SyncState = (*SyncStateStore)(nil)

func NewSyncState(db *gorm.DB, log logrus.FieldLogger) SyncState {
	return &SyncStateStore{dbHandler: db, log: log}
}

func (s *SyncStateStore) getDB(ctx context.Context) *gorm.DB {
	return s.dbHandler.WithContext(ctx)
}

func (s *SyncStateStore) InitialMigration(ctx context.Context) error {
	return s.getDB(ctx).AutoMigrate(&model.SyncState{})
}

func (s *SyncStateStore) Get(ctx context.Context, orgID uuid.UUID, resourceKey string) (*model.SyncState, error) {
	var state model.SyncState
	result := s.getDB(ctx).Take(&state, "org_id = ? AND resource_key = ?", orgID, resourceKey)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, flterrors.ErrResourceNotFound
		}
		return nil, result.Error
	}
	return &state, nil
}

func (s *SyncStateStore) Set(ctx context.Context, orgID uuid.UUID, state *model.SyncState) error {
	if state == nil {
		return fmt.Errorf("syncstate: Set called with nil state")
	}
	state.OrgID = orgID
	return s.getDB(ctx).Save(state).Error
}

func (s *SyncStateStore) List(ctx context.Context, orgID uuid.UUID) ([]model.SyncState, error) {
	var states []model.SyncState
	result := s.getDB(ctx).Where("org_id = ?", orgID).Find(&states)
	if result.Error != nil {
		return nil, result.Error
	}
	return states, nil
}
