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
	ListByFleet(ctx context.Context, orgID uuid.UUID, fleetName string) ([]model.DependencyRef, error)
	ListByDevice(ctx context.Context, orgID uuid.UUID, deviceName string) ([]model.DependencyRef, error)
	ListByRefType(ctx context.Context, orgID uuid.UUID, refType string) ([]model.DependencyRef, error)
	ListByResourceKey(ctx context.Context, orgID uuid.UUID, refType string, cols map[string]string) ([]model.DependencyRef, error)
	DeleteStaleRefs(ctx context.Context, orgID uuid.UUID, fleetName *string, deviceName *string, keepRefTypes []string) error
	DeleteByOwner(ctx context.Context, orgID uuid.UUID, fleetName *string, deviceName *string) error
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

func (s *DependencyRefStore) ListByFleet(ctx context.Context, orgID uuid.UUID, fleetName string) ([]model.DependencyRef, error) {
	var refs []model.DependencyRef
	result := s.getDB(ctx).Where("org_id = ? AND fleet_name = ?", orgID, fleetName).Find(&refs)
	return refs, result.Error
}

func (s *DependencyRefStore) ListByDevice(ctx context.Context, orgID uuid.UUID, deviceName string) ([]model.DependencyRef, error) {
	var refs []model.DependencyRef
	result := s.getDB(ctx).Where("org_id = ? AND device_name = ?", orgID, deviceName).Find(&refs)
	return refs, result.Error
}

func (s *DependencyRefStore) ListByRefType(ctx context.Context, orgID uuid.UUID, refType string) ([]model.DependencyRef, error) {
	var refs []model.DependencyRef
	result := s.getDB(ctx).Where("org_id = ? AND ref_type = ?", orgID, refType).Find(&refs)
	return refs, result.Error
}

var allowedRefColumns = map[string]bool{
	"repository_name":  true,
	"revision":         true,
	"http_suffix":      true,
	"secret_name":      true,
	"secret_namespace": true,
}

// ListByResourceKey finds all fleets/devices referencing a specific external resource.
// The cols map contains column-value pairs for the identity columns (e.g. "repository_name", "revision",
// "secret_name", "secret_namespace"). Column names are validated against an allowlist.
func (s *DependencyRefStore) ListByResourceKey(ctx context.Context, orgID uuid.UUID, refType string, cols map[string]string) ([]model.DependencyRef, error) {
	var refs []model.DependencyRef
	q := s.getDB(ctx).Where("org_id = ? AND ref_type = ?", orgID, refType)
	for col, val := range cols {
		if !allowedRefColumns[col] {
			return nil, fmt.Errorf("dependencyref: invalid column name %q", col)
		}
		q = q.Where(fmt.Sprintf("%s = ?", col), val)
	}
	result := q.Find(&refs)
	return refs, result.Error
}

// DeleteStaleRefs removes dependency_refs rows for a fleet/device that are no longer
// in the current set of ref types. Called after fleet validation or device render
// to clean up refs from removed config providers.
func (s *DependencyRefStore) DeleteStaleRefs(ctx context.Context, orgID uuid.UUID, fleetName *string, deviceName *string, keepRefTypes []string) error {
	q := s.getDB(ctx).Where("org_id = ?", orgID)
	if fleetName != nil {
		q = q.Where("fleet_name = ?", *fleetName)
	}
	if deviceName != nil {
		q = q.Where("device_name = ?", *deviceName)
	}
	if len(keepRefTypes) > 0 {
		q = q.Where("ref_type NOT IN ?", keepRefTypes)
	}
	return q.Delete(&model.DependencyRef{}).Error
}

// DeleteByOwner removes all dependency_refs for a fleet or device.
func (s *DependencyRefStore) DeleteByOwner(ctx context.Context, orgID uuid.UUID, fleetName *string, deviceName *string) error {
	q := s.getDB(ctx).Where("org_id = ?", orgID)
	if fleetName != nil {
		q = q.Where("fleet_name = ?", *fleetName)
	}
	if deviceName != nil {
		q = q.Where("device_name = ?", *deviceName)
	}
	return q.Delete(&model.DependencyRef{}).Error
}
