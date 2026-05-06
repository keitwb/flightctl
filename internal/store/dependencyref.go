// Copyright (c) Flight Control Authors. Licensed under Apache-2.0.

package store

import (
	"context"
	"fmt"

	"github.com/flightctl/flightctl/internal/store/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type DependencyRef interface {
	InitialMigration(ctx context.Context) error
	Upsert(ctx context.Context, orgID uuid.UUID, ref *model.DependencyRef) error
	Delete(ctx context.Context, orgID uuid.UUID, ref *model.DependencyRef) error
}

type DependencyRefStore struct {
	dbHandler *gorm.DB
	log       logrus.FieldLogger
}

var _ DependencyRef = (*DependencyRefStore)(nil)

func NewDependencyRef(db *gorm.DB, log logrus.FieldLogger) DependencyRef {
	return &DependencyRefStore{dbHandler: db, log: log}
}

func (s *DependencyRefStore) getDB(ctx context.Context) *gorm.DB {
	return s.dbHandler.WithContext(ctx)
}

func (s *DependencyRefStore) InitialMigration(ctx context.Context) error {
	return s.getDB(ctx).AutoMigrate(&model.DependencyRef{})
}

func (s *DependencyRefStore) Upsert(ctx context.Context, orgID uuid.UUID, ref *model.DependencyRef) error {
	if ref == nil {
		return fmt.Errorf("dependencyref: Upsert called with nil ref")
	}
	ref.OrgID = orgID
	return s.getDB(ctx).Save(ref).Error
}

// Delete removes a single dependency_ref row identified by its composite primary key.
func (s *DependencyRefStore) Delete(ctx context.Context, orgID uuid.UUID, ref *model.DependencyRef) error {
	if ref == nil {
		return fmt.Errorf("dependencyref: Delete called with nil ref")
	}
	ref.OrgID = orgID
	return s.getDB(ctx).Delete(ref).Error
}
